#!/usr/bin/env python3
import json, time, random, os, signal, threading
import websocket  # pip install websocket-client
from threading import Lock, Event
from fetch_markets import get_tradeable_asset_mappings
from parquet_writer import EventTypeParquetWriter
import hashlib
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s"
)

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_BASE = "https://clob.polymarket.com"
SUB_BATCH = int(os.getenv("SUB_BATCH", "1000"))
SUB_PAUSE_SEC = float(os.getenv("SUB_PAUSE_SEC", ".1"))
MARKETS_UPDATE_INTERVAL = 10
PING_INTERVAL = 30
PING_TIMEOUT = 10

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

writer = EventTypeParquetWriter()

def update_asset_mappings_from_api():
    global subs_version, previous_allowed_asset_ids
    new_assets = 0
    try:
        mappings = get_tradeable_asset_mappings(force_update=False)
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

        if assets_changed:
            with ws_lock:
                subs_version += 1
    except Exception as e:
        print(f"Error updating asset mappings: {e}")
    return max(0, new_assets)

def markets_poll_loop():
    while not should_stop.is_set():
        if should_stop.wait(MARKETS_UPDATE_INTERVAL):
            break
        update_asset_mappings_from_api()

def finalizer_loop():
    # How often to check; 60s is plenty and very cheap
    interval = int(os.getenv("PM_FINALIZE_INTERVAL_SEC", "60"))
    grace = int(os.getenv("PM_GRACE_MINUTES", "15"))
    while not should_stop.is_set():
        writer.flush()
        writer.close_completed_hours(grace_minutes=grace)
        if should_stop.wait(interval):
            break

def send_subscription(ws):
    global subscribed_asset_ids
    with data_lock:
        current_ids = set(allowed_asset_ids)
        new_ids = list(current_ids - subscribed_asset_ids)

    if not new_ids:
        logging.info("No new ids to subscribe")
        return

    for i in range(0, len(new_ids), SUB_BATCH):
        chunk = new_ids[i:i+SUB_BATCH]
        payload = {"assets_ids": chunk, "type": "market", "initial_dump": True}
        raw = json.dumps(payload)
        logging.debug("sub batch %d..%d: ids=%d bytes=%d",
                      i, i+len(chunk)-1, len(chunk), len(raw))
        ws.send(raw)
        time.sleep(SUB_PAUSE_SEC)

    with data_lock:
        subscribed_asset_ids = current_ids
    logging.info("Subscribed %d new ids in %d batches (total now %d)",
                 len(new_ids), (len(new_ids)+SUB_BATCH-1)//SUB_BATCH, len(current_ids))

def on_open(ws):
    global sent_version, last_message_time, backoff
    last_message_time = time.time()
    backoff = BACKOFF_MIN  # Reset backoff on successful connection

    send_subscription(ws)
    with ws_lock:
        sent_version = subs_version

    # Start a subs refresher thread tied to this ws instance
    threading.Thread(target=subs_refresher, args=(ws,), daemon=True).start()

def on_message(ws, msg):
    global last_message_time
    last_message_time = time.time()
    recv_ms = int(last_message_time * 1000)
    try:
        msg = msg.decode("utf-8", errors="replace")
        s = msg.strip()
        if s[0] not in "[{":
            # e.g. "pong"
            return

        payload = json.loads(s)

        if not isinstance(payload, list):  # I added this, not AI. Sometimes the event is sent as a list, sometimes a single item :(
            payload = [payload]
        for d in payload:
            et = d["event_type"]

            common = {
                "timestamp": int(d["timestamp"]),
                "delay": recv_ms - int(d["timestamp"]),
            }

            def intify(p_str):
                return int(float(p_str)*10_000)

            if et == "book":
                h = int.from_bytes(hashlib.blake2s(d["asset_id"].encode("utf-8"), digest_size=8).digest(), "big", signed=True)
                common.update({"asset_hash": h, "book": str(d)})
                writer.write("book", common)

            elif et == "price_change":
                # Flatten each change as a "price" event
                for ch in d["price_changes"]:
                    h = int.from_bytes(hashlib.blake2s(ch["asset_id"].encode("utf-8"), digest_size=8).digest(), "big", signed=True)
                    evt = dict(common)
                    evt.update({
                        "asset_hash": h,
                        "price": intify(ch["price"]),
                        "size": float(ch["size"]),
                        "best_bid": intify(ch["best_bid"]),
                        "best_ask": intify(ch["best_ask"]),
                    })
                    writer.write("price_change_"+ch["side"], evt)

            elif et == "last_trade_price":
                evt = dict(common)
                h = int.from_bytes(hashlib.blake2s(d["asset_id"].encode("utf-8"), digest_size=8).digest(), "big", signed=True)
                evt.update({
                    "asset_hash": h,
                    "price": intify(d["price"]),
                    "size": float(d["size"]),
                    "fee_rate_bps": float(d.get("fee_rate_bps")),
                })
                writer.write("last_trade_price_"+d.get("side"), evt)

            # e.g. tick_size_change
            else:
                evt = dict(common)
                for k, v in d.items():
                    if k not in evt:
                        evt[k] = v
                evt['event_type'] = et
                writer.write("other", evt)
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(e)
        logging.error("on_message failed", exc_info=True)

def on_error(ws, err):
    # Keep this low-noise; the outer loop will recreate
    logging.error("WebSocket error: %r (%s)", err, type(err).__name__, exc_info=True)

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
    writer.close()
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
            suppress_origin=False,
        )
    except Exception as e:
        if not should_stop.is_set():
            print(f"WebSocket run_forever exception: {e}")

def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    update_asset_mappings_from_api()
    threading.Thread(target=markets_poll_loop, daemon=True).start()
    threading.Thread(target=finalizer_loop, daemon=True).start()

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
    writer.close()
if __name__ == "__main__":
    main()
