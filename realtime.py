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
est_tz = pytz.timezone('US/Eastern')
start_time_ms = int(datetime.now(est_tz).timestamp() * 1000)

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
    current_time_est = datetime.now(est_tz)
    print(f'Current EST: {current_time_est.strftime("%Y-%m-%d %H:%M:%S EST")}')
    print()
    print('price | size | side | outcome | timestamp | market')
    print('-' * 100)

def print_trade_line(price_k, size_str, side, outcome, ms_ago, market):
    """Print a formatted trade line"""
    global line_count
    
    print(f'{price_k:>5} | {size_str:>8} | {side:>4} | {outcome:<8} | {ms_ago:>7}ms | {market}')
    line_count += 1
    
    # Show schema every 100 lines
    if line_count % 100 == 0:
        print('-' * 100)
        print('price | size | side | outcome | timestamp | market')
        print('-' * 100)

def on_open(ws):
    print('=== POLYMARKET REAL-TIME TRADING FEED ===')
    print_schema()
    
    subscribe_msg = {
        "assets_ids": ASSET_IDS, 
        "type": "market"
    }
    ws.send(json.dumps(subscribe_msg))

def on_message(ws, msg):
    try:
        current_timestamp_ms = int(datetime.now(est_tz).timestamp() * 1000)
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
                
                # Calculate milliseconds ago
                trade_timestamp_ms = int(data.get('timestamp', current_timestamp_ms))
                ms_ago = current_timestamp_ms - trade_timestamp_ms
                
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
                        
                        # Use current timestamp for order book updates
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

def on_close(ws, close_status_code, close_msg):
    current_time_est = datetime.now(est_tz)
    print()
    print('-' * 100)
    print('price | size | side | outcome | timestamp | market')
    print()
    print(f'Current EST: {current_time_est.strftime("%Y-%m-%d %H:%M:%S EST")}')
    print("WebSocket connection closed")

if __name__ == "__main__":
    print("Loading market data...")
    get_market_titles()
    
    ws = WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nStopping real-time feed...")
        ws.close()