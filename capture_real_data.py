#!/usr/bin/env python3
"""
Capture Polymarket L2 order book (book snapshots + price-level deltas) as NDJSON.
"""

import json
import time
import requests
from websocket import WebSocketApp

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
OUTFILE = "orderbook_clip.jsonl"

ASSET_IDS = []
captured_events = []
start_time = None
CAPTURE_SECONDS = 5  # adjust as needed

def fetch_markets_and_populate_data():
    """Fetch recent trades and collect distinct asset (token) IDs."""
    global ASSET_IDS
    try:
        print("Fetching recent trades to find active assets...")
        resp = requests.get("https://data-api.polymarket.com/trades?limit=10000", timeout=20)
        resp.raise_for_status()
        trades = resp.json()
        print(f"Retrieved {len(trades)} recent trades")

        seen = set()
        for t in trades:
            aid = t.get("asset")
            if aid and aid not in seen:
                seen.add(aid)

        ASSET_IDS = list(seen)
        print(f"Using {len(ASSET_IDS)} asset IDs for data capture")
    except Exception as e:
        print(f"Error fetching markets: {e}")
        ASSET_IDS = []

def write_event(e):
    captured_events.append(e)

def on_message(ws, msg):
    global start_time

    try:
        now_ms = int(time.time() * 1000)
        payload = json.loads(msg)
        events = payload if isinstance(payload, list) else [payload]

        for data in events:
            et = data.get("event_type", "unknown")
            base = {
                "ts_ms": now_ms,
                "event_type": et,
                "asset_id": data.get("asset_id", "unknown"),
                "market": data.get("market"),
            }

            if et == "book":
                # Snapshot of the aggregated L2 book (on subscribe, and after trades affecting the book)
                # Docs sometimes label fields as buys/sells; example shows bids/asks — handle both.
                bids = data.get("bids") or data.get("buys") or []
                asks = data.get("asks") or data.get("sells") or []
                base.update({
                    "bids": bids,   # [{price, size}, ...]
                    "asks": asks,   # [{price, size}, ...]
                    "hash": data.get("hash"),
                    "timestamp": data.get("timestamp"),
                })

            elif et == "price_change":
                # Per-price deltas (new aggregate size at price level)
                base.update({
                    "changes": data.get("changes", []),   # [{price, side, size}, ...]
                    "timestamp": data.get("timestamp"),
                    "hash": data.get("hash"),
                })

            elif et == "tick_size_change":
                base.update({
                    "old_tick_size": data.get("old_tick_size"),
                    "new_tick_size": data.get("new_tick_size"),
                    "timestamp": data.get("timestamp"),
                })

            elif et == "last_trade_price":
                base.update({
                    "price": data.get("price"),
                    "size": data.get("size"),
                    "side": data.get("side"),
                    "fee_rate_bps": data.get("fee_rate_bps"),
                    "timestamp": data.get("timestamp"),
                })

            # Keep any extra fields just in case
            for k, v in data.items():
                if k not in base:
                    base[k] = v

            write_event(base)

        # Stop after CAPTURE_SECONDS
        if start_time and (time.time() - start_time) > CAPTURE_SECONDS:
            print(f"\nCaptured {len(captured_events)} events. Saving to fixture...")
            save_and_exit(ws)

    except Exception as e:
        print(f"Error processing message: {e}")

def save_and_exit(ws):
    try:
        with open(OUTFILE, "w") as f:
            for e in captured_events:
                f.write(json.dumps(e) + "\n")
        print(f"Saved {len(captured_events)} events to {OUTFILE}")
    except Exception as e:
        print(f"Error saving data: {e}")
    finally:
        try:
            ws.close()
        except Exception:
            pass

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_open(ws):
    global start_time
    start_time = time.time()

    if not ASSET_IDS:
        print("No asset IDs loaded. Exiting...")
        ws.close()
        return

    print(f"Subscribing to {len(ASSET_IDS)} asset IDs on Market channel...")
    # As per docs, subscribe with assets_ids + type="market"
    # (auth not required for Market channel)
    sub = {"assets_ids": ASSET_IDS, "type": "market"}
    ws.send(json.dumps(sub))
    print(f"Subscription sent. Capturing for ~{CAPTURE_SECONDS}s...")

if __name__ == "__main__":
    print("Capturing Polymarket L2 order book data...")
    fetch_markets_and_populate_data()

    ws = WebSocketApp(
        WS_BASE,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
    )

    try:
        # Use built-in pings to keep the connection healthy.
        ws.run_forever(ping_interval=20, ping_timeout=10)
    except KeyboardInterrupt:
        print("\nStopping data capture...")
        save_and_exit(ws)
