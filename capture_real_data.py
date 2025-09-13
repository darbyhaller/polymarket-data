#!/usr/bin/env python3
import json, time, random, os, signal, threading
import websocket  # pip install websocket-client
from threading import Lock, Event
from fetch_markets import get_tradeable_asset_mappings
from parquet_writer import write_event, init_writer, close_writer

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_BASE = "https://clob.polymarket.com"
OUTDIR = os.path.join(os.getenv('PARQUET_ROOT', '/var/data/polymarket'), 'parquets')
MARKETS_UPDATE_INTERVAL = 10
PING_INTERVAL = 30
PING_TIMEOUT = 10
FSYNC_EVERY_SEC = 5.0

# If we stop seeing traffic for this long, force-close and reconnect.
STALL_TIMEOUT = 90

# Reconnect backoff
BACKOFF_MIN = 1.0
BACKOFF_MAX = 20.0

# state
data_lock = Lock()
file_lock = Lock()
ws_lock = Lock()

allowed_asset_ids = set()
previous_allowed_asset_ids = set()
asset_to_market = {}
asset_outcome = {}
market_to_first_asset = {}

subscribed_asset_ids = set()
subs_version = 0
sent_version = -1
should_stop = Event()
last_message_time = 0.0
backoff = BACKOFF_MIN  # Global backoff state

def update_asset_mappings_from_api(force_update=False):
    global subs_version, previous_allowed_asset_ids
    new_assets = 0
    try:
        mappings = get_tradeable_asset_mappings(force_update=force_update)
        with data_lock:
            old_size = len(allowed_asset_ids)
            # Store previous set for comparison
            previous_set = previous_allowed_asset_ids.copy()
            
            # Convert to set for proper comparison (mappings returns a list)
            new_asset_set = set(mappings['allowed_asset_ids'])
            
            allowed_asset_ids.clear()
            asset_to_market.clear()
            asset_outcome.clear()
            market_to_first_asset.clear()

            allowed_asset_ids.update(new_asset_set)
            asset_to_market.update(mappings['asset_to_market'])
            asset_outcome.update(mappings['asset_outcome'])
            market_to_first_asset.update(mappings['market_to_first_asset'])

            new_assets = len(allowed_asset_ids) - old_size
            
            # Check if the set composition changed, not just the size
            assets_changed = allowed_asset_ids != previous_set
            previous_allowed_asset_ids = allowed_asset_ids.copy()

            print(f"Updated mappings: {mappings['total_markets']} total markets, "
                  f"{mappings['tradeable_markets']} tradeable, "
                  f"{len(allowed_asset_ids)} subscribed assets")
            
            # Debug logging to understand why assets_changed is True
            if assets_changed:
                added = allowed_asset_ids - previous_set
                removed = previous_set - allowed_asset_ids
                print(f"Assets changed: +{len(added)} added, -{len(removed)} removed")
                if len(added) <= 10:
                    print(f"Added assets: {list(added)}")
                if len(removed) <= 10:
                    print(f"Removed assets: {list(removed)}")
                  
        if assets_changed:
            with ws_lock:
                subs_version += 1
        else:
            print("No asset changes detected - skipping resubscription")
    except Exception as e:
        print(f"Error updating asset mappings: {e}")
    return max(0, new_assets)

def fetch_markets_and_populate_data(initial=False):
    if initial:
        print("Initial market data load...")
    else:
        print("Periodic market update...")
    update_asset_mappings_from_api(force_update=False)

def markets_poll_loop():
    while not should_stop.is_set():
        if should_stop.wait(MARKETS_UPDATE_INTERVAL):
            break
        fetch_markets_and_populate_data(initial=False)

def file_health_monitor():
    """Monitor parquet writer health."""
    while not should_stop.is_set():
        if should_stop.wait(30):  # Check every 30 seconds
            break
        
        # Parquet writer is more robust, but we can still do periodic flushes
        try:
            from parquet_writer import writer
            if writer:
                writer.flush()
                writer.close_completed_hours()
        except Exception as e:
            print(f"Parquet writer flush failed: {e}")

def send_subscription(ws):
    global subscribed_asset_ids
    with data_lock:
        current_ids = set(allowed_asset_ids)
        new_ids = current_ids - subscribed_asset_ids

        if new_ids:
            sub = {"assets_ids": list(new_ids), "type": "market", "initial_dump": True}
            ws.send(json.dumps(sub))
        print(f"Subscribed to {len(new_ids)} new asset IDs. Total: {len(current_ids)}")
        subscribed_asset_ids = current_ids

def on_open(ws):
    global sent_version, last_message_time, backoff
    last_message_time = time.time()
    backoff = BACKOFF_MIN  # Reset backoff on successful connection

    send_subscription(ws)
    with ws_lock:
        sent_version = subs_version
    print("Subscription sent")

    # Start a subs refresher thread tied to this ws instance
    threading.Thread(target=subs_refresher, args=(ws,), daemon=True).start()

def on_message(ws, msg):
    global last_message_time
    last_message_time = time.time()
    recv_ms = int(last_message_time * 1000)
    try:
        if isinstance(msg, (bytes, bytearray)):
            try:
                msg = msg.decode("utf-8", errors="replace")
            except Exception:
                return
        s = msg.strip()
        if not s:
            return
        if s[0] not in "[{":
            # e.g. "pong"
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
                outcome = asset_outcome.get(aid, "")
            base = {
                "recv_ts_ms": recv_ms,
                "asset_id": aid,
                "market": d.get("market"),
                "outcome": outcome,
            }
            if et == "book":
                base.update({
                    "bids": d.get("bids") or d.get("buys") or [],
                    "asks": d.get("asks") or d.get("sells") or [],
                    "timestamp": d.get("timestamp"),
                })
            elif et == "price_change":
                base.update({
                    "changes": d.get("changes", []),
                    "timestamp": d.get("timestamp"),
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
            for k, v in d.items():
                if k not in base:
                    base[k] = v
            write_event(base)
    except Exception as e:
        print(f"on_message error: {e}")

def on_error(ws, err):
    # Keep this low-noise; the outer loop will recreate
    print(f"WebSocket error: {err}")

def on_close(ws, code, msg):
    print(f"WebSocket closed: {code} {msg}")
    global subscribed_asset_ids
    subscribed_asset_ids.clear()

def subs_refresher(ws):
    """Resubscribe if markets thread changed allowed_asset_ids."""
    global sent_version
    while not should_stop.is_set():
        if should_stop.wait(15):
            break
        with ws_lock:
            v = subs_version
        if v != sent_version:
            # Only attempt if socket is open; websocket-client exposes sock and keep_running
            if getattr(ws, "sock", None) and ws.keep_running:
                try:
                    send_subscription(ws)
                    with ws_lock:
                        sent_version = v
                except Exception as e:
                    # Let outer loop handle if this indicates a dead socket
                    print(f"Resubscribe failed: {e}")
            else:
                # Socket is closed; outer loop will reconnect and do a full sub.
                return

def watchdog(ws):
    """Force-close the socket if we haven't seen traffic for STALL_TIMEOUT seconds."""
    while not should_stop.is_set():
        if should_stop.wait(5):
            # Stop signal received, force close the websocket
            try:
                ws.close()
            except:
                pass
            break
        if getattr(ws, "sock", None) and ws.keep_running:
            if time.time() - last_message_time > STALL_TIMEOUT:
                print(f"No data for {STALL_TIMEOUT}s — forcing reconnect")
                try:
                    ws.close()  # triggers exit from run_forever
                except:
                    pass
                return

def handle_signal(signum, frame):
    print(f"Signal {signum}: stopping")
    should_stop.set()
    close_writer()
    # Force exit if signal handler is called multiple times
    if hasattr(handle_signal, '_called'):
        print("Force exit")
        os._exit(1)
    handle_signal._called = True

def create_websocket():
    return websocket.WebSocketApp(
        WS_BASE,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

def run_websocket_with_timeout(ws):
    """Run websocket in a thread that can be interrupted"""
    try:
        ws.run_forever(
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            ping_payload="ping",
            skip_utf8_validation=True,
            suppress_origin=True,
        )
    except Exception as e:
        if not should_stop.is_set():
            print(f"WebSocket run_forever exception: {e}")

def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    init_writer(root=OUTDIR)
    fetch_markets_and_populate_data(initial=True)
    threading.Thread(target=markets_poll_loop, daemon=True).start()
    threading.Thread(target=file_health_monitor, daemon=True).start()

    print("Starting WebSocket with persistent reconnect loop (no rel)...")

    global backoff
    ws = None
    ws_thread = None
    
    while not should_stop.is_set():
        connection_start_time = time.time()
        try:
            print("Creating new WebSocket connection...")
            ws = create_websocket()

            # Start a watchdog for silent sockets
            watchdog_thread = threading.Thread(target=watchdog, args=(ws,), daemon=True)
            watchdog_thread.start()

            print("Attempting WebSocket connection...")
            # Run websocket in a separate thread so we can interrupt it
            ws_thread = threading.Thread(target=run_websocket_with_timeout, args=(ws,), daemon=True)
            ws_thread.start()
            
            # Wait for either the websocket thread to finish or stop signal
            while ws_thread.is_alive() and not should_stop.is_set():
                should_stop.wait(1)  # Check every second
            
            # If stop signal received, force close websocket
            if should_stop.is_set():
                try:
                    ws.close()
                except:
                    pass
                break
            
            # Check if connection ran successfully for a reasonable duration
            connection_duration = time.time() - connection_start_time
            if connection_duration >= 30:  # Reset backoff if connection lasted at least 30 seconds
                backoff = BACKOFF_MIN
                print(f"Connection ran successfully for {connection_duration:.1f}s - reset backoff to {BACKOFF_MIN}s")
            
            # If we get here, the socket closed. Backoff and retry.
            print("Socket ended; backing off before reconnect...")
        except Exception as e:
            if should_stop.is_set():
                break
            print(f"WebSocket exception: {e}")

        # Jittered exponential backoff
        sleep_for = backoff + random.uniform(0, 0.5 * backoff)
        sleep_for = min(sleep_for, BACKOFF_MAX)
        print(f"Reconnecting in {sleep_for:.1f}s...")
        if should_stop.wait(sleep_for):
            break
        backoff = min(backoff * 2, BACKOFF_MAX)
    
    # Ensure websocket is closed when exiting
    if ws and getattr(ws, "sock", None):
        try:
            ws.close()
        except:
            pass

    print("WebSocket loop exited")
    close_writer()

if __name__ == "__main__":
    main()
