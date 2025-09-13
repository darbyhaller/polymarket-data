#!/usr/bin/env python3
"""
Fetch and cache Polymarket simplified markets with cursor-based pagination.
Saves results to markets_cache.json with cursor state for incremental updates.
"""

import json
import time
import requests
import os
from datetime import datetime

CLOB_BASE = "https://clob.polymarket.com"
CACHE_FILE = "markets_cache.json"
CURSOR_FILE = "markets_cursor.txt"

def load_cache():
    """Load existing market cache and cursor state from JSONL format."""
    markets_data = {}
    last_cursor = ""
    
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    data = json.loads(line)
                    if data.get('_type') == 'metadata':
                        last_cursor = data.get('last_cursor', '')
                    elif data.get('_type') == 'market':
                        condition_id = data.pop('condition_id', None)
                        data.pop('_type', None)  # Remove the _type field
                        if condition_id:
                            markets_data[condition_id] = data
                            
            print(f"Loaded {len(markets_data)} cached markets, last cursor: {last_cursor[:20]}...")
        except Exception as e:
            # Try loading as old format for backward compatibility
            try:
                with open(CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    markets_data = cache.get('markets', {})
                    last_cursor = cache.get('last_cursor', '')
                    print(f"Loaded {len(markets_data)} cached markets from old format")
            except:
                print(f"Error loading cache: {e}")
    
    return markets_data, last_cursor

def save_cache(markets_data, cursor):
    """Save market cache and cursor state in JSONL format (one market per line)."""
    try:
        with open(CACHE_FILE, 'w') as f:
            # First line: metadata
            metadata = {
                'last_cursor': cursor,
                'updated_at': datetime.utcnow().isoformat(),
                'total_markets': len(markets_data),
                '_type': 'metadata'
            }
            f.write(json.dumps(metadata, separators=(',', ':')) + '\n')
            
            # Each market on its own line
            for condition_id, market in markets_data.items():
                market_with_id = {
                    'condition_id': condition_id,
                    '_type': 'market',
                    **market
                }
                f.write(json.dumps(market_with_id, separators=(',', ':')) + '\n')
                
        print(f"Saved {len(markets_data)} markets to cache (JSONL format)")
    except Exception as e:
        print(f"Error saving cache: {e}")

def fetch_markets_from_cursor(start_cursor=""):
    """Fetch markets starting from a specific cursor."""
    cursor = start_cursor
    markets_fetched = 0
    
    while True:
        url = f"{CLOB_BASE}/markets"
        params = {"next_cursor": cursor} if cursor else {}
        
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            page = r.json()
            data = page.get("data", [])
            
            if not data:
                return cursor  # no more data; return last valid cursor
                
            markets_fetched += len(data)
            print(f"Fetched page with {len(data)} markets (total: {markets_fetched})")
            
            next_cursor = page.get("next_cursor") or "LTE="
            # Yield the whole page with the next_cursor
            yield data, next_cursor
            
            if next_cursor == "LTE=":
                return cursor  # return the cursor we used for this page, not "LTE="
            cursor = next_cursor
                
        except Exception as e:
            print(f"Error fetching markets: {e}")
            return cursor

def update_markets_cache(full_refresh=False):
    """Update the markets cache incrementally or do full refresh."""
    markets_data, last_cursor = load_cache()
    
    if full_refresh:
        print("Performing full refresh...")
        markets_data = {}
        start_cursor = ""
    else:
        print(f"Performing incremental update from cursor: {last_cursor[:20]}...")
        start_cursor = last_cursor
    
    new_markets = 0
    updated_markets = 0
    final_cursor = start_cursor
    
    for data, next_cursor in fetch_markets_from_cursor(start_cursor):
        for market in data:
            condition_id = market.get("condition_id")
            if not condition_id:
                continue
                
            if condition_id in markets_data:
                updated_markets += 1
            else:
                new_markets += 1
                
            markets_data[condition_id] = market
        # Only advance cursor if it's not the end marker
        if next_cursor != "LTE=":
            final_cursor = next_cursor
    
    print(f"Update complete: {new_markets} new, {updated_markets} updated")
    save_cache(markets_data, final_cursor)
    return markets_data

def get_tradeable_markets():
    """Get markets that meet trading criteria: active, not closed, not archived, accepting orders."""
    markets_data, _ = load_cache()
    tradeable = {}
    
    for condition_id, market in markets_data.items():
        is_active = market.get("active", False)
        is_not_closed = not market.get("closed", True)
        is_not_archived = not market.get("archived", True)
        is_accepting_orders = market.get("accepting_orders", False)
        
        if is_active and is_not_closed and is_not_archived and is_accepting_orders:
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
            cache_age = time.time() - os.path.getmtime(CACHE_FILE)
            if cache_age > 300:  # 5 minutes
                print(f"Cache is {cache_age/60:.1f} minutes old, updating...")
                markets = update_markets_cache(full_refresh=False)
            else:
                markets, _ = load_cache()
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