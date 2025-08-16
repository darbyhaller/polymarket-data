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

# WebSocket URL and recent active asset IDs
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ASSET_IDS = [
    "41973459713215151999702185043345326004708146007379332707173419489400288130968",  # Bitcoin Up/Down
    "37886561608087953818265292905354954599362226068882661611845483805874017292923",  # Everton
    "44617211960566611274104228135196329399423806182011291547349968906058936209209"   # Panthers vs Texans
]

# Global counters and state
line_count = 0
pst_tz = pytz.timezone('US/Pacific')
current_pst_reference = None
reference_timestamp_ms = None

# Asset ID to market title mapping
asset_to_market = {}

def get_market_titles():
    """Get market titles for asset IDs"""
    try:
        response = requests.get('https://data-api.polymarket.com/trades?limit=50')
        trades = response.json()
        
        for trade in trades:
            if trade['asset'] in ASSET_IDS:
                asset_to_market[trade['asset']] = trade['title']
                
    except Exception as e:
        print(f"Warning: Could not fetch market titles: {e}")

def format_size(size):
    """Format size to 3 significant figures"""
    size = float(size)
    if size >= 100:
        return f"{size:.0f}"
    elif size >= 10:
        return f"{size:.1f}"
    else:
        return f"{size:.2f}"

def print_schema():
    """Print the schema header"""
    global current_pst_reference, reference_timestamp_ms
    
    current_pst_reference = datetime.now(pst_tz)
    reference_timestamp_ms = int(current_pst_reference.timestamp() * 1000)
    
    print(f'Current PST: {current_pst_reference.strftime("%Y-%m-%d %H:%M:%S PST")}')
    print()
    print('price | size | side | outcome | timestamp | market')
    print('-' * 100)

def print_trade_line(price_k, size_str, side, outcome, ms_ago, market):
    """Print a formatted trade line"""
    global line_count
    
    print(f'{price_k:>5} | {size_str:>8} | {side:>4} | {outcome:<8} | {ms_ago:>7}ms | {market}')
    line_count += 1
    
    # Show schema every 50 lines
    if line_count % 50 == 0:
        global current_pst_reference, reference_timestamp_ms
        print('-' * 100)
        # Update the PST reference time
        current_pst_reference = datetime.now(pst_tz)
        reference_timestamp_ms = int(current_pst_reference.timestamp() * 1000)
        print(f'Current PST: {current_pst_reference.strftime("%Y-%m-%d %H:%M:%S PST")}')
        print()
        print('price | size | side | outcome | timestamp | market')
        print('-' * 100)

def on_message(ws, msg):
    try:
        current_timestamp_ms = int(datetime.now(pst_tz).timestamp() * 1000)
        events = json.loads(msg)
        if not isinstance(events, list):
            events = [events]
            
        for data in events:
            event_type = data.get('event_type', 'unknown')
            asset_id = data.get('asset_id', 'unknown')
            
            # Get market title
            market = asset_to_market.get(asset_id, f"Asset {asset_id[:8]}...")
            market = market[:45] + "..." if len(market) > 45 else market
            
            if event_type == 'last_trade_price':
                # Real trade execution
                price_k = int(float(data.get('price', 0)) * 1000)
                size = float(data.get('size', 0))
                size_str = format_size(size)
                side = data.get('side', 'N/A')
                
                # Calculate milliseconds since PST reference time
                if reference_timestamp_ms:
                    ms_ago = current_timestamp_ms - reference_timestamp_ms
                else:
                    ms_ago = 0
                
                # Determine outcome (for trades, we don't have outcome info, so use price-based logic)
                if price_k > 500:
                    outcome = "Yes/Up"
                else:
                    outcome = "No/Down"
                
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
                        
                        # Calculate milliseconds since PST reference time
                        if reference_timestamp_ms:
                            ms_ago = current_timestamp_ms - reference_timestamp_ms
                        else:
                            ms_ago = 0
                        
                        # Determine outcome based on price
                        if price_k > 500:
                            outcome = "Yes/Up"
                        else:
                            outcome = "No/Down"
                        
                        print_trade_line(price_k, size_str, side, outcome, ms_ago, market)
            
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

if __name__ == "__main__":
    print("Loading market data...")
    get_market_titles()
    
    print('=== POLYMARKET REAL-TIME TRADING FEED ===')
    print_schema()
    
    def on_open_handler(ws):
        subscribe_msg = {
            "assets_ids": ASSET_IDS,
            "type": "market"
        }
        ws.send(json.dumps(subscribe_msg))
    
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