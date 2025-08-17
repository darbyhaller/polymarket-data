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
CAPTURE_SECONDS = 60  # adjust as needed

# Asset ID to market title mapping and outcome mapping
asset_to_market = {}
asset_outcome = {}

# Track first asset_id seen for each market to avoid duplicates
market_to_first_asset = {}
allowed_asset_ids = set()

# Track seen hashes to avoid duplicate events
seen_hashes = set()

def fetch_markets_and_populate_data():
    """Fetch recent trades and collect distinct asset (token) IDs."""
    global ASSET_IDS, market_to_first_asset, allowed_asset_ids
    try:
        print("Fetching recent trades to find active assets...")
        resp = requests.get("https://data-api.polymarket.com/trades?limit=100000", timeout=20)
        resp.raise_for_status()
        trades = resp.json()
        print(f"Retrieved {len(trades)} recent trades")

        seen = set()
        for t in trades:
            aid = t.get("asset")
            market_title = t.get("title", "").strip()
            
            if aid and aid not in seen:
                seen.add(aid)
                
                # Track first asset_id per market to avoid duplicates
                if market_title and market_title not in market_to_first_asset:
                    market_to_first_asset[market_title] = aid
                    allowed_asset_ids.add(aid)
                    print(f"Market '{market_title[:50]}...' -> first asset_id: {aid}")
                elif market_title and market_to_first_asset.get(market_title) == aid:
                    # This asset_id is the first one for this market
                    allowed_asset_ids.add(aid)
                
                # Populate our mappings while we're iterating
                if market_title:
                    asset_to_market[aid] = market_title[:80]
                
                if t.get("outcome"):
                    asset_outcome[aid] = t["outcome"].title()

        ASSET_IDS = list(seen)
        print(f"Found {len(ASSET_IDS)} total asset IDs")
        print(f"Allowing {len(allowed_asset_ids)} asset IDs (first per market)")
        print(f"Populated {len(asset_to_market)} market titles")
        print(f"Populated {len(asset_outcome)} outcome labels")
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
            asset_id = data.get("asset_id", "unknown")
            
            # Skip events for asset_ids that are not the first seen for their market
            if asset_id not in allowed_asset_ids:
                continue
            
            # Skip events with duplicate hashes
            event_hash = data.get("hash")
            if event_hash:
                if event_hash in seen_hashes:
                    continue
                seen_hashes.add(event_hash)
                
            base = {
                "ts_ms": now_ms,
                "event_type": et,
                "asset_id": asset_id,
                "market": data.get("market"),
                "market_title": asset_to_market.get(asset_id, ""),
                "outcome": asset_outcome.get(asset_id, ""),
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
    # Explicitly request initial book dumps
    sub = {"assets_ids": ASSET_IDS, "type": "market", "initial_dump": True}
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
