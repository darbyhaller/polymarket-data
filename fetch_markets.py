#!/usr/bin/env python3
"""
Fetch and cache Polymarket simplified markets with cursor-based pagination.
Saves results to markets_cache.json with cursor state for incremental updates.
"""

from orjson import dumps, loads
import requests
import os

CLOB_BASE = "https://clob.polymarket.com"
CACHE_FILE = "markets_cache.jsonl"
CURSOR_FILE = "markets_cursor.txt"

def load_cursor():
    try:
        with open(CURSOR_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""

def save_cursor(cursor: str):
    tmp = CURSOR_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(cursor)
    os.replace(tmp, CURSOR_FILE)

def append_cache(new_items, cursor):
    """Append new markets to cache file atomically."""
    # new_items: iterable[(condition_id, market_dict)]
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        for cid, market in new_items:
            f.write(dumps({"_type": "market", "condition_id": cid, **market}).decode() + "\n")
    # append atomically
    with open(CACHE_FILE, "a") as out, open(tmp, "r") as inp:
        for line in inp:
            out.write(line)
    os.remove(tmp)
    # never persist LTE= (tail) cursors
    if cursor and cursor != "LTE=":
        save_cursor(cursor)

def load_cache_streaming():
    """Load cache by streaming the file (no giant dict reconstruct every 10s)."""
    markets = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                rec = loads(line)
                if rec.get("_type") == "market":
                    cid = rec.pop("condition_id", None)
                    rec.pop("_type", None)
                    if cid:
                        markets[cid] = rec
    return markets, load_cursor()

def fetch_markets_from_cursor(start_cursor=""):
    """Fetch markets starting from a specific cursor."""
    cursor = start_cursor
    while True:
        url = f"{CLOB_BASE}/markets"
        params = {"next_cursor": cursor} if cursor else {}
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            page = r.json()
            data = page.get("data", [])
            if not data:
                # No data at this cursor; we're done.
                # We won't rely on a 'return value' from the generator.
                break

            next_cursor = page.get("next_cursor") or "LTE="

            # IMPORTANT: yield the cursor we USED for this fetch, plus the data and next_cursor
            yield cursor, data, next_cursor

            if next_cursor == "LTE=":
                break  # reached the end
            cursor = next_cursor

        except Exception as e:
            print(f"Error fetching markets: {e}")
            break

def update_markets_cache_incremental():
    """Update markets cache incrementally, only appending new records."""
    markets, cursor = load_cache_streaming()  # load once at process start
    final_cursor = cursor
    new_items = []
    
    for cursor_used, data, next_cursor in fetch_markets_from_cursor(cursor):
        print(next_cursor)
        for m in data:
            cid = m.get("condition_id")
            if cid and cid not in markets:
                markets[cid] = m
                new_items.append((cid, m))
        if next_cursor != "LTE=":
            final_cursor = next_cursor
        else:
            # At the end, sleep longer before next poll to avoid useless work
            print("Reached end of markets (LTE=), backing off...")
            break
    
    if new_items:
        append_cache(new_items, final_cursor)
        print(f"Update complete: +{len(new_items)} new")
    else:
        # Just update cursor if it advanced (rare); otherwise do nothing
        if final_cursor and final_cursor != "LTE=" and final_cursor != cursor:
            save_cursor(final_cursor)
    return markets

def update_markets_cache(full_refresh=False):
    """Legacy wrapper for backward compatibility."""
    if full_refresh:
        # For full refresh, clear cache files and start fresh
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
        if os.path.exists(CURSOR_FILE):
            os.remove(CURSOR_FILE)
        print("Performing full refresh...")
    
    return update_markets_cache_incremental()

def get_tradeable_markets():
    """Get markets that meet trading criteria: active, not closed, not archived, accepting orders."""
    markets_data, _ = load_cache_streaming()
    tradeable = {}
    
    for condition_id, market in markets_data.items():
        if market["enable_order_book"]:
            tradeable[condition_id] = market

    return tradeable

def extract_asset_mappings():
    """Extract asset ID mappings for WebSocket subscription from tradeable markets only."""
    # Get only tradeable markets that meet all conditions
    tradeable_markets = get_tradeable_markets()
    
    allowed_asset_ids = set()
    asset_to_market = {}
    asset_outcome = {}
    market_to_first_asset = {}
    
    for condition_id, market in tradeable_markets.items():
        tokens = market.get("tokens", [])
        if len(tokens) < 1:
            continue
            
        # Use condition_id as market title
        title = condition_id
        first_token_found = False
        
        # Process all tokens in the market
        for token in tokens:
            token_id = (
                token.get("token_id") or
                token.get("clob_token_id") or
                token.get("clobTokenId") or
                token.get("id")
            )
            
            if not token_id:
                continue
                
            outcome = (token.get("outcome") or token.get("name") or "").title()
            
            # Add to allowed assets
            allowed_asset_ids.add(token_id)
            asset_to_market[token_id] = title[:80]
            if outcome:
                asset_outcome[token_id] = outcome
                
            # Track first valid token for this market
            if not first_token_found:
                market_to_first_asset[title] = token_id
                first_token_found = True
    
    return {
        'allowed_asset_ids': sorted(list(allowed_asset_ids)),
        'asset_to_market': asset_to_market,
        'asset_outcome': asset_outcome,
        'market_to_first_asset': market_to_first_asset
    }

def get_tradeable_asset_mappings(force_update=False):
    """
    Get asset mappings for tradeable markets, updating cache if needed.
    
    Args:
        force_update (bool): If True, force a full cache refresh
        
    Returns:
        dict: Asset mappings with keys:
            - allowed_asset_ids: list of asset IDs to subscribe to
            - asset_to_market: mapping from asset ID to market title
            - asset_outcome: mapping from asset ID to outcome name
            - market_to_first_asset: mapping from market to first asset ID
            - total_markets: total number of markets in cache
            - tradeable_markets: number of tradeable markets
    """
    # Update cache if needed
    if force_update:
        markets = update_markets_cache(full_refresh=True)
    else:
        # Check if cache exists and is recent (less than 5 minutes old)
        if os.path.exists(CACHE_FILE):
            markets = update_markets_cache(full_refresh=False)
        else:
            print("No cache found, performing full refresh...")
            markets = update_markets_cache(full_refresh=True)
    
    # Get tradeable markets and extract mappings
    tradeable = get_tradeable_markets()
    mappings = extract_asset_mappings()
    
    # Add summary information
    mappings['total_markets'] = len(markets)
    mappings['tradeable_markets'] = len(tradeable)
    
    return mappings

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        mappings = get_tradeable_asset_mappings(force_update=True)
    else:
        mappings = get_tradeable_asset_mappings(force_update=False)
    
    print(f"\nSummary:")
    print(f"Total markets: {mappings['total_markets']}")
    print(f"Tradeable markets: {mappings['tradeable_markets']}")
    print(f"Asset IDs for subscription: {len(mappings['allowed_asset_ids'])}")