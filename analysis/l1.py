#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute L1 (best bid/ask) updates from Polymarket parquet dumps.

- Initializes per-asset order books using the *latest* book snapshot for each asset.
- Processes price_change events strictly in ascending `timestamp` order.
- Emits JSONL events:
  - {event_type: "l1_update", timestamp, asset_id, market_title, outcome, side: "bid"|"ask", price}
  - {event_type: "outage", timestamp}  (timestamp = start of outage = prev_ts + 1000)
- Ignores *.inprogress temp files.

Assumptions:
- Parquet writer stored prices as uint32 = price * 10000, sizes as uint64 = size * 10000.
- `price_change.changes[].size` is treated as the *absolute* remaining size at that price level
  (0 removes the level). If instead it is a delta in your feed, switch UPDATE_MODE below.

Usage:
    python l1.py --root ./parquets --out l1.jsonl
"""

import argparse
import json
import math
import os
import sys
from glob import glob
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

# ---------- Config ----------
PRICE_SCALE = 10000.0     # uint32 -> float
SIZE_SCALE = 10000.0      # uint64 -> float (not used for output, but used to filter > 0 levels)
OUTAGE_MS = 1000

def u32_to_price(u):
    try:
        return round((int(u) / PRICE_SCALE), 4)
    except Exception:
        return 0.0

def normalize_int(x):
    try:
        return int(x)
    except Exception:
        return 0

def is_valid_level(sz_uint):
    """Level is considered present if size > 0."""
    try:
        return int(sz_uint) > 0
    except Exception:
        return False

def list_parquet_files(root, event_type):
    """List finalized parquet files for a given event_type partition."""
    base = os.path.join(root, f"event_type={event_type}")
    # Recursively find *.parquet (ignore *.inprogress)
    return [p for p in glob(os.path.join(base, "**", "*.parquet"), recursive=True)
            if not p.endswith(".inprogress")]

def read_latest_book_per_asset(root):
    """
    Build initial book state per asset from the *latest* book snapshot per asset_id (by timestamp).
    Returns:
      books: dict[asset_id] -> {
          "market_title": str,
          "outcome": str,
          "bids": dict[price_int] -> size_uint,
          "asks": dict[price_int] -> size_uint,
          "best_bid": Optional[int price_int],
          "best_ask": Optional[int price_int],
      }
    """
    files = list_parquet_files(root, "book")
    if not files:
        return {}

    # We only read needed columns
    cols = ["timestamp", "asset_id", "market_title", "outcome", "bids", "asks"]
    # Track the latest row per asset by timestamp
    latest = {}  # asset_id -> (timestamp, row-like dict)

    for fp in files:
        try:
            table = pq.read_table(fp, columns=cols)
        except Exception:
            # Skip any corrupt/partial files
            continue
        if table.num_rows == 0:
            continue

        # Convert to Python-friendly structure just for the latest rows
        # We'll iterate rows and keep the max timestamp per asset
        ts_arr = table.column("timestamp")
        aid_arr = table.column("asset_id")
        mt_arr = table.column("market_title")
        oc_arr = table.column("outcome")
        bids_arr = table.column("bids")
        asks_arr = table.column("asks")

        for i in range(table.num_rows):
            ts = normalize_int(ts_arr[i].as_py())
            aid = aid_arr[i].as_py()
            if not aid:
                continue
            prev = latest.get(aid)
            if prev is None or ts > prev[0]:
                row = {
                    "timestamp": ts,
                    "asset_id": aid,
                    "market_title": mt_arr[i].as_py() or "",
                    "outcome": oc_arr[i].as_py() or "",
                    "bids": bids_arr[i].as_py() or [],
                    "asks": asks_arr[i].as_py() or [],
                }
                latest[aid] = (ts, row)

    books = {}
    for aid, (_, row) in latest.items():
        # Build price->size maps
        bids = {}
        for level in (row.get("bids") or []):
            try:
                p = normalize_int(level.get("price", 0))
                s = normalize_int(level.get("size", 0))
                if is_valid_level(s):
                    bids[p] = s
            except Exception:
                continue
        asks = {}
        for level in (row.get("asks") or []):
            try:
                p = normalize_int(level.get("price", 0))
                s = normalize_int(level.get("size", 0))
                if is_valid_level(s):
                    asks[p] = s
            except Exception:
                continue

        best_bid = max(bids.keys()) if bids else None
        best_ask = min(asks.keys()) if asks else None

        books[aid] = {
            "market_title": row.get("market_title", ""),
            "outcome": row.get("outcome", ""),
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
        }

    return books

def load_all_price_changes(root):
    """
    Load all price_change rows and return a list of dicts sorted by `timestamp`.
    Each dict contains: timestamp, asset_id, market_title, outcome, changes(list).
    """
    files = list_parquet_files(root, "price_change")
    rows = []
    if not files:
        return rows

    cols = ["timestamp", "asset_id", "market_title", "outcome", "changes"]

    for fp in files:
        try:
            table = pq.read_table(fp, columns=cols)
        except Exception:
            continue
        if table.num_rows == 0:
            continue

        ts_arr = table.column("timestamp")
        aid_arr = table.column("asset_id")
        mt_arr = table.column("market_title")
        oc_arr = table.column("outcome")
        ch_arr = table.column("changes")

        for i in range(table.num_rows):
            ts = normalize_int(ts_arr[i].as_py())
            aid = aid_arr[i].as_py()
            if not aid:
                continue
            changes = ch_arr[i].as_py() or []
            rows.append({
                "timestamp": ts,
                "asset_id": aid,
                "market_title": mt_arr[i].as_py() or "",
                "outcome": oc_arr[i].as_py() or "",
                "changes": changes
            })

    # Sort strictly by timestamp
    rows.sort(key=lambda r: r["timestamp"])
    return rows

def compute_l1_updates(root, out_path, pretty=False):
    # 1) Initialize books from latest book per asset
    books = read_latest_book_per_asset(root)

    # 2) Load and sort price_change events
    pc_rows = load_all_price_changes(root)

    # 3) Iterate price_change events to produce updates + outages
    last_pc_ts = None
    emitted = 0

    # Open writer (stdout or file)
    out_fh = sys.stdout if out_path == "-" else open(out_path, "w", encoding="utf-8")

    def emit(obj):
        nonlocal emitted
        if pretty:
            out_fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=False, indent=0).strip() + "\n")
        else:
            out_fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
        emitted += 1

    try:
        for row in pc_rows:
            ts = row["timestamp"]

            # Outage detection starts from the first price_change (t0)
            if last_pc_ts is not None and ts - last_pc_ts > OUTAGE_MS:
                # Emit outage at the time the gap *exceeded* 1000ms
                outage_ts = last_pc_ts + OUTAGE_MS
                emit({"event_type": "outage", "timestamp": outage_ts})

            last_pc_ts = ts

            aid = row["asset_id"]
            mt = row["market_title"] or ""
            oc = row["outcome"] or ""
            changes = row["changes"] or []

            # Ensure a book entry exists even if no prior book snapshot
            book = books.get(aid)
            if book is None:
                book = {
                    "market_title": mt,
                    "outcome": oc,
                    "bids": {},
                    "asks": {},
                    "best_bid": None,
                    "best_ask": None,
                }
                books[aid] = book
            else:
                # Keep freshest non-empty metadata
                if mt and not book["market_title"]:
                    book["market_title"] = mt
                if oc and not book["outcome"]:
                    book["outcome"] = oc

            bids = book["bids"]
            asks = book["asks"]

            # Apply all changes in this event
            for ch in changes:
                try:
                    side = (ch.get("side") or "").lower()
                    p_int = normalize_int(ch.get("price", 0))
                    s_uint = normalize_int(ch.get("size", 0))
                except Exception:
                    continue
                if side not in ("bid", "ask"):
                    continue

                levels = bids if side == "bid" else asks

                if is_valid_level(s_uint):
                    levels[p_int] = s_uint
                else:
                    # Remove level when size == 0
                    if p_int in levels:
                        del levels[p_int]

            # Recompute bests
            prev_best_bid = book["best_bid"]
            prev_best_ask = book["best_ask"]
            new_best_bid = max(bids.keys()) if bids else None
            new_best_ask = min(asks.keys()) if asks else None

            # Emit updates for changed sides
            if new_best_bid != prev_best_bid and new_best_bid is not None:
                emit({
                    "event_type": "l1_update",
                    "timestamp": ts,
                    "asset_id": aid,
                    "market_title": book["market_title"],
                    "outcome": book["outcome"],
                    "side": "bid",
                    "price": u32_to_price(new_best_bid),
                })
            if new_best_ask != prev_best_ask and new_best_ask is not None:
                emit({
                    "event_type": "l1_update",
                    "timestamp": ts,
                    "asset_id": aid,
                    "market_title": book["market_title"],
                    "outcome": book["outcome"],
                    "side": "ask",
                    "price": u32_to_price(new_best_ask),
                })

            # Commit new bests
            book["best_bid"] = new_best_bid
            book["best_ask"] = new_best_ask

    finally:
        if out_fh is not sys.stdout:
            out_fh.close()

    return emitted

def main():
    ap = argparse.ArgumentParser(description="Generate L1 updates & outage events from Polymarket parquet dumps.")
    ap.add_argument("--root", default="./parquets", help="Root folder containing parquet partitions (default: ./parquets)")
    ap.add_argument("--out", default="l1.jsonl", help="Output path (JSONL). Use '-' for stdout. (default: l1.jsonl)")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON (slower, bigger files)")
    args = ap.parse_args()

    emitted = compute_l1_updates(args.root, args.out, pretty=args.pretty)
    print(f"Wrote {emitted} events to {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()
