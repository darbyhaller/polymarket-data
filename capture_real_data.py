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
CLOB_BASE = "https://clob.polymarket.com"
# OUTFILE = "/data/polybook/orderbook_clip.jsonl"      # adjust if needed
OUTFILE = "./orderbook_clip.jsonl"      # adjust if needed
MARKETS_UPDATE_INTERVAL = 60                          # seconds (less frequent than trades)
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

def fetch_all_simplified_markets():
    """Yield pages from /simplified-markets (handles cursor paging)."""
    cursor = ""  # empty = beginning; 'LTE=' means end per docs
    while True:
        url = f"{CLOB_BASE}/simplified-markets"
        params = {"next_cursor": cursor} if cursor else {}
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        page = r.json()
        data = page.get("data", [])
        if not data:
            break
        yield data
        cursor = page.get("next_cursor") or "LTE="
        if cursor == "LTE=":
            break

def fetch_markets_and_populate_data(initial=False):
    """
    Populate allowed_asset_ids / asset_to_market / asset_outcome using Simplified Markets.
    Unlike the /trades approach, this sees markets even if there are no trades yet.
    """
    global subs_version
    new_firsts = 0
    total_markets_processed = 0
    active_tradeable_count = 0
    last_market_title = "None"
    
    try:
        seen_conditions = set()
        with data_lock:
            # optional: clear on first init so we fully rebuild from catalog
            if initial:
                allowed_asset_ids.clear()
                asset_to_market.clear()
                asset_outcome.clear()
                market_to_first_asset.clear()

        page_num = 0
        for markets in fetch_all_simplified_markets():
            page_num += 1
            page_active_tradeable = 0
            
            with data_lock:
                for m in markets:
                    total_markets_processed += 1
                    
                    # Update last market title for ALL markets (not just tradeable ones)
                    cond = m.get("condition_id") or m.get("conditionId")
                    if cond:
                        last_market_title = cond[:50]
                    
                    # Check if market meets all active trading conditions
                    is_active = m.get("active", False)
                    is_not_closed = not m.get("closed", True)
                    is_not_archived = not m.get("archived", True)
                    is_accepting_orders = m.get("accepting_orders", False)
                    
                    # Debug: print actual values for first market of first page
                    if page_num == 1 and total_markets_processed == 1:
                        print(f"Debug first market: active={is_active}, closed={m.get('closed')}, archived={m.get('archived')}, accepting_orders={is_accepting_orders}")
                    
                    if is_active and is_not_closed and is_not_archived and is_accepting_orders:
                        active_tradeable_count += 1
                        page_active_tradeable += 1
                    
                    # fields per docs: condition_id, tokens (length 2), active/closed, etc.
                    if not cond or cond in seen_conditions:
                        continue
                    seen_conditions.add(cond)

                    tokens = m.get("tokens") or []
                    if len(tokens) < 1:
                        continue

                    # Each token should carry a CLOB token id (asset id)
                    def tok_id(t):
                        return (
                            t.get("token_id")
                            or t.get("clob_token_id")
                            or t.get("clobTokenId")
                            or t.get("id")
                        )

                    # pick first token per market to avoid dup books
                    first_token = tok_id(tokens[0])
                    if not first_token:
                        continue

                    # Use condition_id as market identifier since there's no title/question
                    title = cond
                    # outcome names from tokens (e.g., "Chiefs", "Yes", "No")
                    outcome0 = (tokens[0].get("outcome") or tokens[0].get("name") or "").title()

                    if title and title not in market_to_first_asset:
                        market_to_first_asset[title] = first_token
                        allowed_asset_ids.add(first_token)
                        asset_to_market[first_token] = title[:80]
                        if outcome0:
                            asset_outcome[first_token] = outcome0
                        new_firsts += 1
                    else:
                        # if we've already mapped this title → first token, still ensure metadata
                        if title:
                            asset_to_market[first_token] = title[:80]
                            if outcome0:
                                asset_outcome[first_token] = outcome0

            if initial:
                print(f"Page {page_num}: {len(markets)} markets, {page_active_tradeable} active/tradeable this page, {active_tradeable_count} total active/tradeable, last: {last_market_title}")

        if initial:
            print(f"Init from simplified-markets: processed={total_markets_processed} markets, active/tradeable={active_tradeable_count}, allowed_assets={len(allowed_asset_ids)}")
        elif new_firsts:
            with ws_lock:
                subs_version += 1
            print(f"New first-per-market assets: +{new_firsts} (total {len(allowed_asset_ids)})")

    except Exception as e:
        print(f"fetch_markets (simplified) error: {e}")

def markets_poll_loop():
    while not should_stop.is_set():
        time.sleep(MARKETS_UPDATE_INTERVAL)
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
        ping_payload="ping"
    )
    rel.dispatch()  # blocks until aborted by signal

if __name__ == "__main__":
    main()
