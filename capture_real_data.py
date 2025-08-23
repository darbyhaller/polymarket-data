#!/usr/bin/env python3
"""
Capture Polymarket L2 order book (book snapshots + price-level deltas) as NDJSON.
"""

import json
import time
import requests
import threading
from threading import Lock, Event
from websocket import WebSocketApp
import random

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
OUTFILE = "orderbook_clip.jsonl"

ASSET_IDS = []
start_time = None
CAPTURE_SECONDS = 3600 * 8  # adjust as needed
TRADES_UPDATE_INTERVAL = 10  # seconds between trades API calls

# WebSocket reconnection settings
MAX_RECONNECT_ATTEMPTS = 50  # Maximum reconnection attempts
RECONNECT_BASE_DELAY = 1  # Base delay for exponential backoff (seconds)
RECONNECT_MAX_DELAY = 60  # Maximum delay between reconnection attempts
CONNECTION_TIMEOUT = 30  # Connection timeout in seconds
PING_INTERVAL = 15  # Ping interval in seconds
PING_TIMEOUT = 5  # Ping timeout in seconds

# Connection state tracking
ws_connected = Event()
should_stop = Event()
reconnect_count = 0
last_message_time = None

# Asset ID to market title mapping and outcome mapping
asset_to_market = {}
asset_outcome = {}

# Track first asset_id seen for each market to avoid duplicates
market_to_first_asset = {}
allowed_asset_ids = set()

# Track seen hashes to avoid duplicate events
seen_hashes = set()

# Thread safety locks
data_lock = Lock()
file_lock = Lock()

# File handle for live writing
outfile_handle = None

def calculate_reconnect_delay(attempt):
    """Calculate exponential backoff delay with jitter."""
    delay = min(RECONNECT_BASE_DELAY * (2 ** attempt), RECONNECT_MAX_DELAY)
    # Add jitter to avoid thundering herd
    jitter = random.uniform(0.1, 0.3) * delay
    return delay + jitter

def fetch_markets_and_populate_data(initial_load=True):
    """Fetch recent trades and collect distinct asset (token) IDs."""
    global ASSET_IDS, market_to_first_asset, allowed_asset_ids
    try:
        if initial_load:
            print("Fetching recent trades to find active assets...")
            trades_to_load = "1000000"
        else:
            trades_to_load = "5000"
            # usually 10k/minute, this handles bursts of 3x (30k/minute)
        resp = requests.get("https://data-api.polymarket.com/trades?limit="+trades_to_load, timeout=20)
        resp.raise_for_status()
        trades = resp.json()
        if initial_load:
            print(f"Retrieved {len(trades)} recent trades")

        seen = set()
        new_assets = 0
        
        with data_lock:
            for t in trades:
                aid = t.get("asset")
                market_title = t.get("title", "").strip()
                
                if aid and aid not in seen:
                    seen.add(aid)
                    
                    # Track first asset_id per market to avoid duplicates
                    if market_title and market_title not in market_to_first_asset:
                        market_to_first_asset[market_title] = aid
                        allowed_asset_ids.add(aid)
                        new_assets += 1
                        if initial_load:
                            print(f"Market '{market_title[:50]}...' -> first asset_id: {aid}")
                    elif market_title and market_to_first_asset.get(market_title) == aid:
                        # This asset_id is the first one for this market
                        if aid not in allowed_asset_ids:
                            allowed_asset_ids.add(aid)
                            new_assets += 1
                    
                    # Populate our mappings while we're iterating
                    if market_title:
                        asset_to_market[aid] = market_title[:80]
                    
                    if t.get("outcome"):
                        asset_outcome[aid] = t["outcome"].title()

            ASSET_IDS = list(seen)
            
        if initial_load:
            print(f"Found {len(ASSET_IDS)} total asset IDs")
            print(f"Allowing {len(allowed_asset_ids)} asset IDs (first per market)")
            print(f"Populated {len(asset_to_market)} market titles")
            print(f"Populated {len(asset_outcome)} outcome labels")
        elif new_assets > 0:
            print(f"Updated: Found {new_assets} new allowed asset IDs (total: {len(allowed_asset_ids)})")
            
    except Exception as e:
        print(f"Error fetching markets: {e}")
        if initial_load:
            ASSET_IDS = []

def periodic_trades_fetcher():
    """Periodically fetch trades to keep asset IDs updated."""
    while True:
        time.sleep(TRADES_UPDATE_INTERVAL)
        try:
            fetch_markets_and_populate_data(initial_load=False)
        except Exception as e:
            print(f"Error in periodic trades fetcher: {e}")

def write_event(e):
    """Write event immediately to file."""
    global outfile_handle
    if outfile_handle:
        with file_lock:
            try:
                outfile_handle.write(json.dumps(e) + "\n")
                outfile_handle.flush()  # Ensure data is written immediately
            except Exception as ex:
                print(f"Error writing event to file: {ex}")

def on_message(ws, msg):
    global start_time, last_message_time

    try:
        last_message_time = time.time()
        now_ms = int(time.time() * 1000)
        payload = json.loads(msg)
        events = payload if isinstance(payload, list) else [payload]

        for data in events:
            et = data.get("event_type", "unknown")
            asset_id = data.get("asset_id", "unknown")
            
            # Thread-safe access to allowed_asset_ids
            with data_lock:
                is_allowed = asset_id in allowed_asset_ids
                market_title = asset_to_market.get(asset_id, "")
                outcome = asset_outcome.get(asset_id, "")
            
            # Skip events for asset_ids that are not the first seen for their market
            if not is_allowed:
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
                "market_title": market_title,
                "outcome": outcome,
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
            print(f"\nCapture time completed. Closing...")
            save_and_exit(ws)

    except Exception as e:
        print(f"Error processing message: {e}")

def save_and_exit(ws):
    global outfile_handle
    try:
        if outfile_handle:
            outfile_handle.close()
            print(f"Data stream closed and saved to {OUTFILE}")
    except Exception as e:
        print(f"Error closing file: {e}")
    finally:
        try:
            ws.close()
        except Exception:
            pass

def on_error(ws, error):
    print(f"WebSocket error: {error}")
    ws_connected.clear()

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} - {close_msg}")
    ws_connected.clear()

def on_open(ws):
    global start_time, reconnect_count, last_message_time
    
    if start_time is None:
        start_time = time.time()
    
    last_message_time = time.time()
    ws_connected.set()
    
    if reconnect_count > 0:
        print(f"WebSocket reconnected successfully (attempt {reconnect_count})")
    else:
        print("WebSocket connected")

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
    
    # Reset reconnect count on successful connection
    reconnect_count = 0

def create_websocket():
    """Create a new WebSocket instance with all handlers."""
    return WebSocketApp(
        WS_BASE,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

def connection_monitor():
    """Monitor connection health and trigger reconnection if needed."""
    global last_message_time
    
    while not should_stop.is_set():
        time.sleep(30)  # Check every 30 seconds
        
        if should_stop.is_set():
            break
            
        # Check if we haven't received messages for too long
        if last_message_time and (time.time() - last_message_time) > 120:  # 2 minutes
            print("No messages received for 2 minutes, connection may be stale")
            ws_connected.clear()
        
        # Check if capture time is exceeded
        if start_time and (time.time() - start_time) > CAPTURE_SECONDS:
            print(f"\nCapture time completed. Stopping...")
            should_stop.set()
            break

def run_with_reconnection():
    """Run WebSocket with automatic reconnection logic."""
    global reconnect_count
    
    while not should_stop.is_set() and reconnect_count < MAX_RECONNECT_ATTEMPTS:
        try:
            ws = create_websocket()
            
            if reconnect_count > 0:
                delay = calculate_reconnect_delay(reconnect_count)
                print(f"Reconnecting in {delay:.1f} seconds (attempt {reconnect_count + 1}/{MAX_RECONNECT_ATTEMPTS})...")
                time.sleep(delay)
            
            print("Starting WebSocket connection...")
            ws.run_forever(
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                ping_payload="ping"
            )
            
        except Exception as e:
            print(f"WebSocket connection failed: {e}")
        
        # If we get here, the connection was lost
        ws_connected.clear()
        
        if should_stop.is_set():
            break
            
        reconnect_count += 1
        
        if reconnect_count >= MAX_RECONNECT_ATTEMPTS:
            print(f"Maximum reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached. Giving up.")
            break
        
        print(f"Connection lost. Will attempt to reconnect...")
    
    # Clean shutdown
    save_and_exit(None)

if __name__ == "__main__":
    print("Capturing Polymarket L2 order book data...")
    
    # Open file for live writing
    try:
        outfile_handle = open(OUTFILE, "w")
        print(f"Opened {OUTFILE} for live writing")
    except Exception as e:
        print(f"Error opening output file: {e}")
        exit(1)
    
    # Fetch initial markets data
    fetch_markets_and_populate_data()
    
    # Start periodic trades fetcher thread
    trades_thread = threading.Thread(target=periodic_trades_fetcher, daemon=True)
    trades_thread.start()
    print(f"Started periodic trades fetcher (updates every {TRADES_UPDATE_INTERVAL}s)")
    
    # Start connection monitor thread
    monitor_thread = threading.Thread(target=connection_monitor, daemon=True)
    monitor_thread.start()
    print("Started connection health monitor")

    try:
        # Run WebSocket with automatic reconnection
        run_with_reconnection()
    except KeyboardInterrupt:
        print("\nStopping data capture...")
        should_stop.set()
        save_and_exit(None)
    finally:
        should_stop.set()
        if outfile_handle:
            try:
                outfile_handle.close()
            except:
                pass
