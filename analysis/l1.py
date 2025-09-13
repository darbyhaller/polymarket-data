#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute L1 (best bid/ask) updates from Polymarket parquet dumps.

- Initializes per-asset order books using the *latest* book snapshot for each asset.
- Processes price_change events strictly in ascending `timestamp` order.
- Emits JSONL events:
  - {event_type: "l1_update", timestamp, asset_id, market_title, outcome, side: "buy"|"sell", price}
  - {event_type: "outage", timestamp}  (timestamp = start of outage = prev_ts + 1000)
- Ignores *.inprogress temp files.

Assumptions:
- Parquet writer stored prices as uint32 = price * 10000, sizes as uint64 = size * 10000.

Usage:
    python analysis/l1.py --root ./parquets --out l1.jsonl --verbose
"""

import argparse
import json
import os
import sys
from glob import glob

import pyarrow.parquet as pq

# ---------- Config ----------
PRICE_SCALE = 10000.0     # uint32 -> float
OUTAGE_MS = 1000

def u32_to_price(u):
    return round(int(u) / PRICE_SCALE, 4)

def normalize_int(x):
    return int(x)

def list_parquet_files(root, event_type):
    base = os.path.join(root, f"event_type={event_type}")
    return [p for p in glob(os.path.join(base, "**", "*.parquet"), recursive=True)
            if not p.endswith(".inprogress")]

def read_latest_book_per_asset(root, verbose=False):
    files = list_parquet_files(root, "book")
    if verbose:
        print(f"[init] book files: {len(files)}", file=sys.stderr)
    if not files:
        return {}

    cols = ["timestamp", "asset_id", "market_title", "outcome", "bids", "asks"]
    latest = {}  # asset_id -> (timestamp, rowdict)

    for fp in files:
        table = pq.read_table(fp, columns=cols)
        if table.num_rows == 0:
            continue
        ts, aid, mt, oc, bids, asks = [table[c] for c in cols]
        for i in range(table.num_rows):
            t = normalize_int(ts[i].as_py()); a = aid[i].as_py()
            if not a: 
                continue
            prev = latest.get(a)
            if prev is None or t > prev[0]:
                latest[a] = (t, {
                    "timestamp": t,
                    "asset_id": a,
                    "market_title": (mt[i].as_py() or ""),
                    "outcome": (oc[i].as_py() or ""),
                    "bids": bids[i].as_py() or [],
                    "asks": asks[i].as_py() or [],
                })

    books = {}
    for a, (_, row) in latest.items():
        bmap, amap = {}, {}
        for lvl in (row["bids"] or []):
            p = normalize_int(lvl.get("price", 0)); s = normalize_int(lvl.get("size", 0))
            bmap[p] = s
        for lvl in (row["asks"] or []):
            p = normalize_int(lvl.get("price", 0)); s = normalize_int(lvl.get("size", 0))
            amap[p] = s
        books[a] = {
            "market_title": row["market_title"],
            "outcome": row["outcome"],
            "bids": bmap,
            "asks": amap,
            "best_bid": max(bmap) if bmap else None,
            "best_ask": min(amap) if amap else None,
        }
    print(f"[init] books built: {len(books)} assets", file=sys.stderr)
    return books

def load_all_price_changes(root, verbose=False):
    print('hi')
    files = list_parquet_files(root, "price_change")
    print(f"[load] price_change files: {len(files)}", file=sys.stderr)
    rows = []
    cols = ["timestamp", "asset_id", "market_title", "outcome", "changes"]
    total_rows = 0
    for fp in files:
        table = pq.read_table(fp, columns=cols)
        total_rows += table.num_rows
        if table.num_rows == 0:
            continue
        ts, aid, mt, oc, ch = [table[c] for c in cols]
        for i in range(table.num_rows):
            t = normalize_int(ts[i].as_py()); a = aid[i].as_py()
            if not a:
                continue
            rows.append({
                "timestamp": t,
                "asset_id": a,
                "market_title": (mt[i].as_py() or ""),
                "outcome": (oc[i].as_py() or ""),
                "changes": ch[i].as_py() or []
            })
    rows.sort(key=lambda r: r["timestamp"])
    if verbose:
        print(f"[load] price_change rows loaded: {len(rows)} (raw: {total_rows})", file=sys.stderr)
    return rows

def compute_l1_updates(root, out_path, pretty=False, verbose=False):
    books = read_latest_book_per_asset(root, verbose=verbose)
    pc_rows = load_all_price_changes(root, verbose=verbose)

    last_pc_ts = None
    emitted = 0
    outages = 0
    updates = 0

    out_fh = sys.stdout if out_path == "-" else open(out_path, "w", encoding="utf-8")
    def emit(obj):
        nonlocal emitted
        if pretty:
            out_fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        else:
            out_fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
        emitted += 1
    
    for row in pc_rows:
        ts = row["timestamp"]
        if last_pc_ts is not None and ts - last_pc_ts > OUTAGE_MS:
            emit({"event_type": "outage", "timestamp": last_pc_ts + OUTAGE_MS})
            outages += 1
        last_pc_ts = ts

        aid = row["asset_id"]
        mt = row["market_title"] or ""
        oc = row["outcome"] or ""
        changes = row["changes"] or []

        book = books.get(aid)
        if book is None:
            book = {"market_title": mt, "outcome": oc, "bids": {}, "asks": {}, "best_bid": None, "best_ask": None}
            books[aid] = book
        else:
            if mt and not book["market_title"]:
                book["market_title"] = mt
            if oc and not book["outcome"]:
                book["outcome"] = oc

        bids, asks = book["bids"], book["asks"]

        for ch in changes:
            side = ch["side"]
            p_int = normalize_int(ch["price"])
            s_uint = normalize_int(ch["size"])
            levels = bids if side == "buy" else asks
            if s_uint > 0:
                levels[p_int] = s_uint
            else:
                levels.pop(p_int, None)

        prev_bid, prev_ask = book["best_bid"], book["best_ask"]
        new_bid = max(bids) if bids else None
        new_ask = min(asks) if asks else None

        if new_bid != prev_bid and new_bid is not None:
            emit({
                "event_type": "l1_update",
                "timestamp": ts,
                "asset_id": aid,
                "market_title": book["market_title"],
                "outcome": book["outcome"],
                "side": "buy",
                "price": new_bid,
            })
            updates += 1
        if new_ask != prev_ask and new_ask is not None:
            emit({
                "event_type": "l1_update",
                "timestamp": ts,
                "asset_id": aid,
                "market_title": book["market_title"],
                "outcome": book["outcome"],
                "side": "sell",
                "price": new_ask,
            })
            updates += 1

        book["best_bid"], book["best_ask"] = new_bid, new_ask

    print(f"[done] emitted: {emitted} (updates: {updates}, outages: {outages})", file=sys.stderr)
    return emitted

def main():
    ap = argparse.ArgumentParser(description="Generate L1 updates & outage events from Polymarket parquet dumps.")
    ap.add_argument("--root", default="./parquets", help="Root folder with parquet partitions")
    ap.add_argument("--out", default="l1.jsonl", help="Output JSONL path; use '-' for stdout")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    ap.add_argument("--verbose", action="store_true", help="Log progress to stderr")
    args = ap.parse_args()

    emitted = compute_l1_updates(args.root, args.out, pretty=args.pretty, verbose=args.verbose)
    print(f"Wrote {emitted} events to {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()
