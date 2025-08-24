#!/usr/bin/env python3
"""
Polymarket L2 order book -> NDJSON (auto-reconnect via websocket-client + rel).
- Dynamic (re)subscription: refresh when new "first-per-market" assets appear
- Dedupe by event hash
- Periodic fsync for durability
- Graceful shutdown
"""

import json, time, random, os, signal, threading, sys
import requests
import websocket, rel  # pip install websocket-client rel
from threading import Lock, Event
from datetime import datetime
from fetch_markets import get_tradeable_asset_mappings

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_BASE = "https://clob.polymarket.com"
# OUTFILE = "/data/polybook/orderbook_clip.jsonl"      # adjust if needed
OUTFILE = "./orderbook_clip.jsonl"      # adjust if needed
MARKETS_UPDATE_INTERVAL = 10                          # seconds (catch new markets quickly)
PING_INTERVAL = 30                                    # Increased from 15 to reduce ping frequency
PING_TIMEOUT = 10                                     # Increased from 5 to allow more time for pong
FSYNC_EVERY_SEC = 5.0

# state
data_lock = Lock()
file_lock = Lock()
ws_lock = Lock()

allowed_asset_ids = set()        # first asset_id per market
asset_to_market = {}
asset_outcome = {}
market_to_first_asset = {}
seen_hashes = set()

# Track subscriptions for delta updates
subscribed_asset_ids = set()     # currently subscribed asset IDs
subs_version = 0                 # bump when allowed_asset_ids changes
sent_version = -1
should_stop = Event()
last_message_time = 0.0
is_first_subscription = True     # track if this is the first subscription

outfile_handle = None
_last_fsync = 0.0

def fs_open():
    global outfile_handle, _last_fsync
    os.makedirs(os.path.dirname(OUTFILE), exist_ok=True)
    outfile_handle = open(OUTFILE, "a", encoding="utf-8", buffering=1)
    _last_fsync = time.time()
    print(f"Opened {OUTFILE} (append)")

def fs_close():
    global outfile_handle
    with file_lock:
        if outfile_handle:
            try:
                outfile_handle.flush()
                os.fsync(outfile_handle.fileno())
            except Exception:
                pass
            try:
                outfile_handle.close()
            except Exception:
                pass
            outfile_handle = None

def write_event(obj):
    global outfile_handle, _last_fsync
    if outfile_handle is None: 
        return
    line = json.dumps(obj, separators=(",", ":")) + "\n"
    with file_lock:
        outfile_handle.write(line)
        now = time.time()
        if now - _last_fsync >= FSYNC_EVERY_SEC:
            try:
                os.fsync(outfile_handle.fileno())
            except Exception:
                pass
            _last_fsync = now

def update_asset_mappings_from_api(force_update=False):
    """
    Update asset mappings using the fetch_markets API.
    
    Args:
        force_update (bool): If True, force a full cache refresh
        
    Returns:
        int: Number of new assets added
    """
    global subs_version
    new_assets = 0
    
    try:
        # Get fresh mappings from the API
        mappings = get_tradeable_asset_mappings(force_update=force_update)
        
        with data_lock:
            # Store current size for comparison
            old_size = len(allowed_asset_ids)
            
            # Update all mappings
            allowed_asset_ids.clear()
            asset_to_market.clear()
            asset_outcome.clear()
            market_to_first_asset.clear()
            
            # Populate from API response
            allowed_asset_ids.update(mappings['allowed_asset_ids'])
            asset_to_market.update(mappings['asset_to_market'])
            asset_outcome.update(mappings['asset_outcome'])
            market_to_first_asset.update(mappings['market_to_first_asset'])
            
            new_assets = len(allowed_asset_ids) - old_size
            
            print(f"Updated mappings: {mappings['total_markets']} total markets, "
                  f"{mappings['tradeable_markets']} tradeable, "
                  f"{len(allowed_asset_ids)} subscribed assets")
        
        if new_assets != 0:  # Changed from > 0 to != 0 to handle both additions and removals
            with ws_lock:
                subs_version += 1
            if new_assets > 0:
                print(f"Market update: +{new_assets} new assets")
            else:
                print(f"Market update: {abs(new_assets)} assets removed")
                
    except Exception as e:
        print(f"Error updating asset mappings: {e}")
        
    return max(0, new_assets)  # Return 0 if negative

def fetch_markets_and_populate_data(initial=False):
    """
    Populate allowed_asset_ids using the fetch_markets API.
    """
    if initial:
        print("Initial market data load...")
        update_asset_mappings_from_api(force_update=False)
    else:
        print("Periodic market update...")
        update_asset_mappings_from_api(force_update=False)

def markets_poll_loop():
    while not should_stop.is_set():
        time.sleep(MARKETS_UPDATE_INTERVAL)
        fetch_markets_and_populate_data(initial=False)

def send_subscription(ws):
    global subscribed_asset_ids, is_first_subscription
    
    with data_lock:
        current_ids = set(allowed_asset_ids)
        
        # Calculate deltas
        new_ids = current_ids - subscribed_asset_ids
        removed_ids = subscribed_asset_ids - current_ids
        
        # If this is the first subscription or we have a major change, do full subscription
        if is_first_subscription or len(new_ids) + len(removed_ids) > len(current_ids) * 0.5:
            # Full subscription with initial dump
            sub = {"assets_ids": list(current_ids), "type": "market", "initial_dump": True}
            ws.send(json.dumps(sub))
            subscribed_asset_ids = current_ids.copy()
            print(f"Full subscription to {len(current_ids)} asset IDs (initial_dump=True)")
            is_first_subscription = False
        else:
            # Delta updates
            if new_ids:
                # Subscribe to new IDs with initial dump
                sub = {"assets_ids": list(new_ids), "type": "market", "initial_dump": True}
                ws.send(json.dumps(sub))
                print(f"Subscribed to {len(new_ids)} new asset IDs (initial_dump=True)")
            
            if removed_ids:
                # Unsubscribe from removed IDs
                unsub = {"assets_ids": list(removed_ids), "type": "market", "unsubscribe": True}
                ws.send(json.dumps(unsub))
                print(f"Unsubscribed from {len(removed_ids)} asset IDs")
            
            # Update tracking
            subscribed_asset_ids = current_ids.copy()
            
            if not new_ids and not removed_ids:
                print(f"No subscription changes needed ({len(current_ids)} assets)")

# WebSocket callbacks
def on_open(ws):
    global sent_version, last_message_time, is_first_subscription
    last_message_time = time.time()
    print("WebSocket connected")
    if not allowed_asset_ids:
        print("No allowed asset IDs; closing")
        ws.close()
        return
    
    # Reset first subscription flag on new connection
    is_first_subscription = True
    send_subscription(ws)
    with ws_lock:
        sent_version = subs_version
    print("Subscription sent")

def on_message(ws, msg):
    global last_message_time
    last_message_time = time.time()
    recv_ms = int(last_message_time * 1000)

    try:
        # Handle bytes → str conversion
        if isinstance(msg, (bytes, bytearray)):
            try:
                msg = msg.decode("utf-8", errors="replace")
            except Exception:
                return  # skip un-decodable frames

        s = msg.strip()
        if not s:
            return  # empty frame, ignore

        # Some servers send non-JSON text like "pong" or "ok"
        if s[0] not in "[{":
            # low-noise one-line preview for debugging
            print(f"Non-JSON text frame (ignored): {s[:120]!r}")
            return

        try:
            payload = json.loads(s)
        except json.JSONDecodeError as e:
            print(f"JSON decode failed at pos {e.pos}: {s[:120]!r}")
            return

        events = payload if isinstance(payload, list) else [payload]
        for d in events:
            et = d.get("event_type", "unknown")
            aid = d.get("asset_id")
            with data_lock:
                if aid not in allowed_asset_ids:
                    continue
                h = d.get("hash")
                if h:
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)
                title = asset_to_market.get(aid, "")
                outcome = asset_outcome.get(aid, "")
            base = {
                "recv_ts_ms": recv_ms,
                "event_type": et,
                "asset_id": aid,
                "market": d.get("market"),
                "market_title": title,
                "outcome": outcome,
            }
            if et == "book":
                base.update({
                    "bids": d.get("bids") or d.get("buys") or [],
                    "asks": d.get("asks") or d.get("sells") or [],
                    "hash": d.get("hash"),
                    "timestamp": d.get("timestamp"),
                })
            elif et == "price_change":
                base.update({
                    "changes": d.get("changes", []),
                    "timestamp": d.get("timestamp"),
                    "hash": d.get("hash"),
                })
            elif et == "tick_size_change":
                base.update({
                    "old_tick_size": d.get("old_tick_size"),
                    "new_tick_size": d.get("new_tick_size"),
                    "timestamp": d.get("timestamp"),
                })
            elif et == "last_trade_price":
                base.update({
                    "price": d.get("price"),
                    "size": d.get("size"),
                    "side": d.get("side"),
                    "fee_rate_bps": d.get("fee_rate_bps"),
                    "timestamp": d.get("timestamp"),
                })
            # keep unknown extras
            for k, v in d.items():
                if k not in base:
                    base[k] = v
            write_event(base)
    except Exception as e:
        print(f"on_message error: {e}")

def on_data(ws, data, opcode, fin):
    """Handle different WebSocket frame types, only process text frames."""
    # OPCODE_TEXT = 0x1, only process text frames
    if opcode == 0x1:
        on_message(ws, data)
    # Ignore binary, ping, pong, and other control frames silently

def on_error(ws, err):
    print(f"WebSocket error: {err}")

def on_close(ws, code, msg):
    print(f"WebSocket closed: {code} {msg}")

def subs_refresher(ws):
    """Resubscribe if trades thread discovered new first-per-market assets."""
    global sent_version
    while ws.sock and ws.keep_running and not should_stop.is_set():
        time.sleep(15)
        with ws_lock:
            v = subs_version
        if v != sent_version and ws.sock and ws.keep_running:
            try:
                send_subscription(ws)
                with ws_lock:
                    sent_version = v
            except Exception as e:
                print(f"Resubscribe failed: {e}")

def handle_signal(signum, frame):
    print(f"Signal {signum}: stopping")
    should_stop.set()
    rel.abort()  # stop dispatcher runloop
    fs_close()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    fs_open()
    fetch_markets_and_populate_data(initial=True)

    # background polling of markets to grow allowed_asset_ids
    threading.Thread(target=markets_poll_loop, daemon=True).start()

    # websocket app + rel dispatcher (auto-reconnect)
    ws = websocket.WebSocketApp(
        WS_BASE,
        on_open=lambda w: (on_open(w), threading.Thread(target=subs_refresher, args=(w,), daemon=True).start()),
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_data=on_data,  # Handle different frame types
    )

    # Note: rel handles reconnect automatically when reconnect>0
    # You still get ping/pong and ping timeouts handled by websocket-client.
    print("Starting WebSocket (auto-reconnect with rel)...")
    ws.run_forever(
        dispatcher=rel,
        reconnect=5,                 # seconds between reconnect attempts
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_TIMEOUT,
        ping_payload="ping",
        skip_utf8_validation=True,   # Skip UTF-8 validation for better performance
        suppress_origin=True         # Suppress origin header
    )
    rel.dispatch()  # blocks until aborted by signal

if __name__ == "__main__":
    main()
