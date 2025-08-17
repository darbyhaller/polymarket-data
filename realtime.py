#!/usr/bin/env python3
"""
Polymarket Real-Time Trading Feed
WebSocket-based low-latency feed using the same format as main.py
"""

import json
import time
from datetime import datetime
import pytz
from websocket import WebSocketApp
import requests

# WebSocket URL
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Global variables
ASSET_IDS = []

# Global counters and state
line_count = 0
pst_tz = pytz.timezone('US/Pacific')
current_pst_reference = None
reference_timestamp_ms = None

# Last trade data for duplicate filtering
last_trade = None

# Asset ID to market title mapping and outcome mapping
asset_to_market = {}
asset_outcome = {}

def fetch_markets_and_populate_data():
    """
    Fetch recently active markets from trades API and populate token IDs, titles, and outcomes.
    Adapted from get_markets.py logic.
    """
    global ASSET_IDS
    
    try:
        print("Fetching recent trades to find active markets and token IDs...")
        
        # Get comprehensive trades data
        response = requests.get('https://data-api.polymarket.com/trades?limit=10000', timeout=20)
        response.raise_for_status()
        trades = response.json()
        
        print(f"Retrieved {len(trades)} recent trades")
        
        # Group trades by condition_id to find unique markets
        markets_by_condition = {}
        
        for trade in trades:
            condition_id = trade.get("conditionId")
            if not condition_id:
                continue
                
            if condition_id not in markets_by_condition:
                markets_by_condition[condition_id] = {
                    "condition_id": condition_id,
                    "question": trade.get("title", ""),
                    "token_ids": set(),
                    "outcomes": set(),
                    "trade_count": 0
                }
            
            market = markets_by_condition[condition_id]
            
            # Add token_id and outcome
            asset_id = trade.get("asset")
            if asset_id:
                market["token_ids"].add(asset_id)
                
                # Populate our mappings
                if trade.get("title"):
                    asset_to_market[asset_id] = trade["title"][:45]
                
                if trade.get("outcome"):
                    asset_outcome[asset_id] = trade["outcome"].title()
            
            if trade.get("outcome"):
                market["outcomes"].add(trade.get("outcome"))
            
            market["trade_count"] += 1
        
        # Extract all unique token IDs
        all_token_ids = []
        for market in markets_by_condition.values():
            all_token_ids.extend(list(market["token_ids"]))
        
        # Remove duplicates while preserving order
        ASSET_IDS = list(dict.fromkeys(all_token_ids))
        
        print(f"Found {len(markets_by_condition)} unique markets")
        print(f"Extracted {len(ASSET_IDS)} unique token IDs")
        print(f"Populated {len(asset_to_market)} market titles")
        print(f"Populated {len(asset_outcome)} outcome labels")
        
    except Exception as e:
        print(f"Error fetching markets and token data: {e}")
        ASSET_IDS = []

def format_size(size):
    """Format size to 3 significant figures"""
    size = float(size)
    if size >= 100:
        return f"{size:.0f}"
    elif size >= 10:
        return f"{size:.1f}"
    else:
        return f"{size:.2f}"

def update_pst_reference_and_print_schema():
    """Update PST reference time and print schema header"""
    global current_pst_reference, reference_timestamp_ms
    
    current_pst_reference = datetime.now(pst_tz)
    reference_timestamp_ms = int(current_pst_reference.timestamp() * 1000)
    
    print(f'Current PST: {current_pst_reference.strftime("%Y-%m-%d %H:%M:%S PST")}')
    print()
    print('price | size | side | outcome | ms since last TS | market')
    print('-' * 100)

def print_schema():
    """Print the initial schema header"""
    update_pst_reference_and_print_schema()

def get_outcome_label(asset_id):
    """Get outcome label for asset ID"""
    return asset_outcome.get(asset_id, "Unknown")

def calculate_ms_since_reference(current_timestamp_ms):
    """Calculate milliseconds since PST reference time"""
    return current_timestamp_ms - reference_timestamp_ms if reference_timestamp_ms else 0

def print_trade_line(price_k, size_str, side, outcome, ms_ago, market):
    """Print a formatted trade line with duplicate filtering"""
    global line_count, last_trade
    
    # Create current trade data
    current_trade = {
        'price_k': price_k,
        'size_str': size_str,
        'side': side,
        'outcome': outcome,
        'ms_ago': ms_ago,
        'market': market
    }
    
    # Check for back-to-back duplicates
    if last_trade is not None:
        is_duplicate = (
            # Same market
            current_trade['market'] == last_trade['market'] and
            # Same size
            current_trade['size_str'] == last_trade['size_str'] and
            # Opposite sides
            ((current_trade['side'] == 'BUY' and last_trade['side'] == 'SELL') or
             (current_trade['side'] == 'SELL' and last_trade['side'] == 'BUY')) and
            # Prices add up to 1000
            current_trade['price_k'] + last_trade['price_k'] == 1000
        )
        
        if is_duplicate:
            # Skip printing this duplicate
            return
    
    # Not a duplicate, print the trade
    print(f'{price_k:>5} | {size_str:>8} | {side:>4} | {outcome:<8} | {ms_ago:>4} | {market}')
    line_count += 1
    
    # Update last_trade for next comparison
    last_trade = current_trade
    
    # Show schema every 50 lines
    if line_count % 50 == 0:
        print('-' * 100)
        update_pst_reference_and_print_schema()

def process_event_data(data, current_timestamp_ms):
    """Process individual event data and print trade line if applicable"""
    event_type = data.get('event_type', 'unknown')
    asset_id = data.get('asset_id', 'unknown')
    
    # Get market title
    market = asset_to_market.get(asset_id, f"Asset {asset_id[:8]}...")
    market = market[:120] + "..." if len(market) > 120 else market
    
    ms_ago = calculate_ms_since_reference(current_timestamp_ms)
    
    if event_type == 'last_trade_price':
        # Real trade execution
        price_k = int(float(data.get('price', 0)) * 1000)
        size_str = format_size(float(data.get('size', 0)))
        side = data.get('side', 'N/A')
        outcome = get_outcome_label(asset_id)
        
        print_trade_line(price_k, size_str, side, outcome, ms_ago, market)
        
    elif event_type == 'price_change':
        # Order book updates (only show significant changes)
        changes = data.get('changes', [])
        for change in changes:
            size = float(change.get('size', 0))
            
            # Only show non-zero size changes (actual new orders)
            if size > 0:
                price_k = int(float(change.get('price', 0)) * 1000)
                size_str = format_size(size)
                side = change.get('side', 'N/A')
                outcome = get_outcome_label(asset_id)
                
                print_trade_line(price_k, size_str, side, outcome, ms_ago, market)

def on_message(ws, msg):
    try:
        current_timestamp_ms = int(datetime.now(pst_tz).timestamp() * 1000)
        events = json.loads(msg)
        if not isinstance(events, list):
            events = [events]
            
        for data in events:
            process_event_data(data, current_timestamp_ms)
            
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

if __name__ == "__main__":
    print("Loading market data...")
    fetch_markets_and_populate_data()
    
    print('=== POLYMARKET REAL-TIME TRADING FEED ===')
    print_schema()
    
    def on_open_handler(ws):
        if not ASSET_IDS:
            print("No asset IDs loaded. Exiting...")
            return
            
        print(f"Subscribing to {len(ASSET_IDS)} token IDs...")
        subscribe_msg = {
            "assets_ids": ASSET_IDS,
            "type": "market",
            "initial_dump": False  # Disable initial order book dump to avoid flood
        }
        ws.send(json.dumps(subscribe_msg))
        print("Subscription message sent successfully!")
    
    ws = WebSocketApp(
        WS_URL,
        on_open=on_open_handler,
        on_message=on_message,
        on_error=on_error
    )
    
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nStopping real-time feed...")
        ws.close()