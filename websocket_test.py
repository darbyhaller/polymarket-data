#!/usr/bin/env python3
"""
Quick WebSocket test for Polymarket real-time data
"""

import json
import time
from websocket import WebSocketApp

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Recent active asset IDs from trades
ASSET_IDS = [
    "41973459713215151999702185043345326004708146007379332707173419489400288130968",  # Bitcoin Up/Down
    "37886561608087953818265292905354954599362226068882661611845483805874017292923",  # Everton
    "44617211960566611274104228135196329399423806182011291547349968906058936209209"   # Panthers vs Texans
]

def on_open(ws):
    print("WebSocket connected!")
    subscribe_msg = {
        "assets_ids": ASSET_IDS, 
        "type": "market"
    }
    ws.send(json.dumps(subscribe_msg))
    print(f"Subscribed to {len(ASSET_IDS)} assets")

def on_message(ws, msg):
    try:
        # Messages come as arrays of events
        events = json.loads(msg)
        if not isinstance(events, list):
            events = [events]
            
        for data in events:
            event_type = data.get('event_type', 'unknown')
            asset_id = data.get('asset_id', 'unknown')
            
            print(f"[{event_type}] Asset: {asset_id[:8]}...")
            
            if event_type == 'last_trade_price':
                price = float(data.get('price', 0)) * 1000  # Convert to thousands
                size = float(data.get('size', 0))
                side = data.get('side', 'N/A')
                print(f"  🔥 TRADE: {price} @ {size:.1f} ({side})")
                
            elif event_type == 'price_change':
                changes = data.get('changes', [])
                for change in changes:
                    price = float(change.get('price', 0)) * 1000
                    size = float(change.get('size', 0))
                    side = change.get('side', 'N/A')
                    print(f"  📊 UPDATE: {price} @ {size:.1f} ({side})")
                    
            elif event_type == 'book':
                bids = data.get('bids', [])
                asks = data.get('asks', [])
                print(f"  📖 BOOK: {len(bids)} bids, {len(asks)} asks")
                if bids:
                    best_bid = float(bids[0]['price']) * 1000
                    bid_size = float(bids[0]['size'])
                    print(f"    Best bid: {best_bid} @ {bid_size:.1f}")
                if asks:
                    best_ask = float(asks[0]['price']) * 1000
                    ask_size = float(asks[0]['size'])
                    print(f"    Best ask: {best_ask} @ {ask_size:.1f}")
            
    except Exception as e:
        print(f"Error parsing message: {e}")
        print(f"Raw msg: {msg[:200]}...")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

if __name__ == "__main__":
    print("Starting WebSocket test...")
    ws = WebSocketApp(
        URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nStopping WebSocket test...")
        ws.close()