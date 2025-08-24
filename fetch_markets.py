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
    """Load existing market cache and cursor state."""
    markets_data = {}
    last_cursor = ""
    
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
                markets_data = cache.get('markets', {})
                last_cursor = cache.get('last_cursor', '')
                print(f"Loaded {len(markets_data)} cached markets, last cursor: {last_cursor[:20]}...")
        except Exception as e:
            print(f"Error loading cache: {e}")
    
    return markets_data, last_cursor

def save_cache(markets_data, cursor):
    """Save market cache and cursor state."""
    cache = {
        'markets': markets_data,
        'last_cursor': cursor,
        'updated_at': datetime.utcnow().isoformat(),
        'total_markets': len(markets_data)
    }
    
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f, separators=(',', ':'))
        print(f"Saved {len(markets_data)} markets to cache")
    except Exception as e:
        print(f"Error saving cache: {e}")

def fetch_markets_from_cursor(start_cursor=""):
    """Fetch markets starting from a specific cursor."""
    cursor = start_cursor
    markets_fetched = 0
    
    while True:
        url = f"{CLOB_BASE}/simplified-markets"
        params = {"next_cursor": cursor} if cursor else {}
        
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            page = r.json()
            data = page.get("data", [])
            
            if not data:
                break
                
            markets_fetched += len(data)
            print(f"Fetched page with {len(data)} markets (total: {markets_fetched})")
            
            # Yield each market with cursor info
            for market in data:
                yield market, cursor
            
            cursor = page.get("next_cursor") or "LTE="
            if cursor == "LTE=":
                break
                
        except Exception as e:
            print(f"Error fetching markets: {e}")
            break
    
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
    
    for market, cursor in fetch_markets_from_cursor(start_cursor):
        condition_id = market.get("condition_id")
        if not condition_id:
            continue
            
        if condition_id in markets_data:
            updated_markets += 1
        else:
            new_markets += 1
            
        markets_data[condition_id] = market
        final_cursor = cursor
    
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
    """Extract asset ID mappings for WebSocket subscription."""
    markets_data, _ = load_cache()
    
    allowed_asset_ids = set()
    asset_to_market = {}
    asset_outcome = {}
    market_to_first_asset = {}
    
    for condition_id, market in markets_data.items():
        tokens = market.get("tokens", [])
        if len(tokens) < 1:
            continue
            
        # Get first token ID
        first_token = (
            tokens[0].get("token_id") or
            tokens[0].get("clob_token_id") or
            tokens[0].get("clobTokenId") or
            tokens[0].get("id")
        )
        
        if not first_token:
            continue
            
        # Use condition_id as market title
        title = condition_id
        outcome = (tokens[0].get("outcome") or tokens[0].get("name") or "").title()
        
        if title not in market_to_first_asset:
            market_to_first_asset[title] = first_token
            allowed_asset_ids.add(first_token)
            asset_to_market[first_token] = title[:80]
            if outcome:
                asset_outcome[first_token] = outcome
    
    return {
        'allowed_asset_ids': list(allowed_asset_ids),
        'asset_to_market': asset_to_market,
        'asset_outcome': asset_outcome,
        'market_to_first_asset': market_to_first_asset
    }

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        markets = update_markets_cache(full_refresh=True)
    else:
        markets = update_markets_cache(full_refresh=False)
    
    tradeable = get_tradeable_markets()
    mappings = extract_asset_mappings()
    
    print(f"\nSummary:")
    print(f"Total markets: {len(markets)}")
    print(f"Tradeable markets: {len(tradeable)}")
    print(f"Asset IDs for subscription: {len(mappings['allowed_asset_ids'])}")