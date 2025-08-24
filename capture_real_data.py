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

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
# OUTFILE = "/data/polybook/orderbook_clip.jsonl"      # adjust if needed
OUTFILE = "./orderbook_clip.jsonl"      # adjust if needed
TRADES_UPDATE_INTERVAL = 10                           # seconds
PING_INTERVAL = 15
PING_TIMEOUT = 5
FSYNC_EVERY_SEC = 1.0

# state
data_lock = Lock()
file_lock = Lock()
ws_lock = Lock()

allowed_asset_ids = set()        # first asset_id per market
asset_to_market = {}
asset_outcome = {}
market_to_first_asset = {}
seen_hashes = set()

subs_version = 0                 # bump when allowed_asset_ids grows
sent_version = -1
should_stop = Event()
last_message_time = 0.0

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

def fetch_markets_and_populate_data(initial=False):
    """Poll recent trades and maintain first-per-market asset set."""
    global subs_version
    try:
        limit = "1000000" if initial else "5000"
        r = requests.get(f"https://data-api.polymarket.com/trades?limit={limit}", timeout=20)
        r.raise_for_status()
        trades = r.json()
        new_firsts = 0
        with data_lock:
            seen_local = set()
            for t in trades:
                aid = t.get("asset")
                title = (t.get("title") or "").strip()
                if not aid or aid in seen_local:
                    continue
                seen_local.add(aid)
                if title and title not in market_to_first_asset:
                    market_to_first_asset[title] = aid
                    allowed_asset_ids.add(aid)
                    new_firsts += 1
                elif title and market_to_first_asset.get(title) == aid:
                    if aid not in allowed_asset_ids:
                        allowed_asset_ids.add(aid)
                        new_firsts += 1
                if title:
                    asset_to_market[aid] = title[:80]
                if t.get("outcome"):
                    asset_outcome[aid] = t["outcome"].title()
        if initial:
            print(f"Init: markets={len(market_to_first_asset)} allowed_assets={len(allowed_asset_ids)}")
        elif new_firsts:
            with ws_lock:
                # bump version so the ws refresher resubscribes
                subs_version += 1
            print(f"New first-per-market assets: +{new_firsts} (total {len(allowed_asset_ids)})")
    except Exception as e:
        print(f"fetch_markets error: {e}")

def trades_poll_loop():
    while not should_stop.is_set():
        time.sleep(TRADES_UPDATE_INTERVAL)
        fetch_markets_and_populate_data(initial=False)

def send_subscription(ws):
    with data_lock:
        ids = list(allowed_asset_ids)
    sub = {"assets_ids": ids, "type": "market", "initial_dump": True}
    ws.send(json.dumps(sub))
    print(f"(Re)subscribed to {len(ids)} asset IDs")

# WebSocket callbacks
def on_open(ws):
    global sent_version, last_message_time
    last_message_time = time.time()
    print("WebSocket connected")
    if not allowed_asset_ids:
        print("No allowed asset IDs; closing")
        ws.close()
        return
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

    # background polling of trades to grow allowed_asset_ids
    threading.Thread(target=trades_poll_loop, daemon=True).start()

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
        ping_payload="ping"
    )
    rel.dispatch()  # blocks until aborted by signal

if __name__ == "__main__":
    main()
