#!/usr/bin/env python3
"""
Preprocess L2 order book data to L1 (top-of-book) using DuckDB
=============================================================

Features
--------
- **Global sort by (key, ts)** in parallel via DuckDB (uses all cores)
- **Outage detection** with exact lookahead (no watermarking)
- **Per-stream outages** via --key-field (default: asset_id). Use "GLOBAL" for whole firehose
- Reads partitioned directories or loose files; supports gzip inputs
- Writes to single output file or RotatingGzipWriter directory
- Optionally write outages interleaved with L1 or to a separate file via --outages-output

Usage examples
--------------
  # Parallel in-memory/global sort on a multi-core laptop
  python preprocess_to_l1_sorted.py /var/data/polymarket l1.jsonl --threads 0

  # Cloud writer + separate outages file
  python preprocess_to_l1_sorted.py /var/data/polymarket \
      --cloud-output /var/data/polymarket/l1 \
      --outages-output /var/data/polymarket/outages.jsonl
"""

import argparse
import orjson as json
import os
import glob
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Union
from isal import igzip
import duckdb
from tqdm import tqdm

# Import your existing writer utility
from writer import RotatingGzipWriter

# ---------------------------
# Micro-price utilities
# ---------------------------
SCALE = 1_000_000

def p_to_u(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    try:
        return int(round(float(s) * SCALE))
    except (ValueError, TypeError):
        return None

def u_to_s(u: Optional[int]) -> Optional[str]:
    if u is None:
        return None
    return f"{u / SCALE:.6f}"

# ---------------------------
# L1 processing
# ---------------------------
@dataclass
class L1Quote:
    ts_ms: int
    asset_id: str
    market: str
    market_title: str
    outcome: str
    best_bid_u: Optional[int] = None
    best_ask_u: Optional[int] = None
    best_bid_sz: Optional[str] = None
    best_ask_sz: Optional[str] = None

    def to_dict(self) -> Dict:
        flip = self.outcome.lower() in ("down", "no")
        bu, au = self.best_bid_u, self.best_ask_u
        if flip and bu is not None and au is not None:
            bu2 = SCALE - au
            au2 = SCALE - bu
            bsz, asz = self.best_ask_sz, self.best_bid_sz
        else:
            bu2, au2 = bu, au
            bsz, asz = self.best_bid_sz, self.best_ask_sz
        spread = u_to_s(au2 - bu2) if (bu2 is not None and au2 is not None) else None
        mid = u_to_s((au2 + bu2)//2) if (bu2 is not None and au2 is not None) else None
        return {
            "ts_ms": self.ts_ms,
            "asset_id": self.asset_id,
            "market": self.market,
            "market_title": self.market_title,
            "outcome": self.outcome,
            "best_bid_price": u_to_s(bu2),
            "best_bid_size": bsz,
            "best_ask_price": u_to_s(au2),
            "best_ask_size": asz,
            "spread": spread,
            "mid_price": mid,
        }

class L1Processor:
    def __init__(self) -> None:
        self.l1_state: Dict[str, L1Quote] = {}

    @staticmethod
    def _best_levels(bids, asks):
        best_bid = max(bids, key=lambda x: float(x.get("price", "0"))) if bids else None
        best_ask = min(asks, key=lambda x: float(x.get("price", "1e9"))) if asks else None
        return best_bid, best_ask

    def process_book(self, ev: Dict) -> Optional[L1Quote]:
        asset_id = ev.get("asset_id")
        if not asset_id:
            return None
        bids = ev.get("bids", [])
        asks = ev.get("asks", [])
        best_bid, best_ask = self._best_levels(bids, asks)
        t = ev.get("timestamp") or ev.get("ts_ms")
        if t is None:
            return None
        ts_ms = int(t)
        q = L1Quote(
            ts_ms=ts_ms,
            asset_id=asset_id,
            market=ev.get("market", ""),
            market_title=ev.get("market_title", ""),
            outcome=ev.get("outcome", ""),
        )
        if best_bid:
            q.best_bid_u = p_to_u(best_bid.get("price"))
            q.best_bid_sz = best_bid.get("size")
        if best_ask:
            q.best_ask_u = p_to_u(best_ask.get("price"))
            q.best_ask_sz = best_ask.get("size")
        self.l1_state[asset_id] = q
        return q

    def process_price_change(self, ev: Dict) -> Optional[L1Quote]:
        asset_id = ev.get("asset_id")
        if not asset_id or asset_id not in self.l1_state:
            return None
        q = self.l1_state[asset_id]
        updated = False
        for ch in ev.get("changes", []):
            price = ch.get("price")
            side = ch.get("side")
            size = ch.get("size")
            if not price or not side:
                continue
            p = p_to_u(price)
            s_pos = (size is not None and float(size) > 0.0)
            if side == "buy":
                cur = q.best_bid_u or -1
                if p is not None and p >= cur:
                    if s_pos:
                        q.best_bid_u, q.best_bid_sz = p, size
                        updated = True
                    elif p == cur:
                        q.best_bid_u, q.best_bid_sz = None, None
                        updated = True
            elif side == "sell":
                cur = q.best_ask_u if q.best_ask_u is not None else 10**15
                if p is not None and p <= cur:
                    if s_pos:
                        q.best_ask_u, q.best_ask_sz = p, size
                        updated = True
                    elif p == cur:
                        q.best_ask_u, q.best_ask_sz = None, None
                        updated = True
        if updated:
            t = ev.get("timestamp") or ev.get("ts_ms")
            if t is not None:
                q.ts_ms = int(t)
            return q
        return None

    def process_event(self, ev: Dict) -> Optional[L1Quote]:
        if ev.get("event_type") == "book":
            return self.process_book(ev)
        elif ev.get("event_type") == "price_change":
            return self.process_price_change(ev)
        return None

# ---------------------------
# IO utils
# ---------------------------
def open_file_smart(path: str):
    if path.endswith(".gz"):
        return igzip.open(path, "rt", encoding="utf-8", newline="")
    return open(path, "r", encoding="utf-8", newline="")

def discover_input_files(input_path: str,
                         exclude_dir_names: Tuple[str, ...] = ("l1", "temp_output.jsonl")) -> List[str]:
    """
    Find only *files* that end with .jsonl or .jsonl.gz.
    Skip any directory subtrees whose basename matches exclude_dir_names.
    """
    import os, glob

    files: List[str] = []

    def is_excluded_dir(path: str) -> bool:
        base = os.path.basename(path.rstrip(os.sep))
        return base in exclude_dir_names

    # Single-file input
    if os.path.isfile(input_path) and (input_path.endswith(".jsonl") or input_path.endswith(".jsonl.gz")):
        return [input_path]

    # Directory input
    if os.path.isdir(input_path):
        # First, include top-level loose files (but not dirs that look like files)
        for pattern in ("*.jsonl", "*.jsonl.gz", "events-*.jsonl", "events-*.jsonl.gz"):
            for p in glob.glob(os.path.join(input_path, pattern)):
                if os.path.isfile(p):
                    files.append(p)

        # Now walk recursively, pruning excluded subtrees
        for root, dirnames, filenames in os.walk(input_path):
            # Prune excluded dirs in-place
            dirnames[:] = [d for d in dirnames if not is_excluded_dir(os.path.join(root, d))]
            # Collect files
            for fn in filenames:
                if fn.endswith(".jsonl") or fn.endswith(".jsonl.gz"):
                    full = os.path.join(root, fn)
                    if os.path.isfile(full):  # guard against odd FS entries
                        files.append(full)

    files = sorted(set(files))
    if not files:
        raise FileNotFoundError(f"No input files found in: {input_path}")
    return files

def dumps_text(obj) -> str:
    return json.dumps(obj).decode("utf-8")

# ---------------------------
# DuckDB sort + processing
# ---------------------------
from tqdm import tqdm
import duckdb
import os

def duckdb_sorted_row_iter(input_files: List[str], key_field: Optional[str], threads: int = 0):
    # keep only actual files, sorted
    files = sorted(os.path.abspath(p) for p in input_files if os.path.isfile(p))
    if not files:
        raise FileNotFoundError("No input files after filtering to real files.")
    print(f"Using {len(files)} files (first 3): {files[:3]}")

    con = duckdb.connect()
    if threads > 0:
        con.execute(f"PRAGMA threads = {threads}")

    # columns we care about (explicit schema avoids inference churn)
    COLUMNS_SPEC = """{
        timestamp: BIGINT,
        ts_ms: BIGINT,
        event_type: VARCHAR,
        asset_id: VARCHAR,
        bids: JSON,
        asks: JSON,
        changes: JSON,
        market: VARCHAR,
        market_title: VARCHAR,
        outcome: VARCHAR
    }"""

    def q(s: str) -> str:  # SQL-escape single quotes
        return s.replace("'", "''")

    # 1) Create empty table with correct schema (read zero rows from first file)
    con.execute(
        f"""
        CREATE TEMP TABLE ev AS
        SELECT * FROM read_json('{q(files[0])}', columns={COLUMNS_SPEC}, format='newline_delimited')
        WHERE 1=0
        """
    )

    # 2) Ingest with a progress bar (file-level)
    with tqdm(total=len(files), desc="Reading files", unit="file") as pbarf:
        for path in files:
            con.execute(
                f"""
                INSERT INTO ev
                SELECT * FROM read_json('{q(path)}', columns={COLUMNS_SPEC}, format='newline_delimited')
                """
            )
            pbarf.update(1)

    key_expr = f"COALESCE(CAST({key_field} AS VARCHAR), 'GLOBAL')" if key_field else "'GLOBAL'"

    # 3) Count rows once for accurate row-level bar
    total_rows = con.execute(
        "SELECT COUNT(*) FROM ev WHERE COALESCE(timestamp, ts_ms) IS NOT NULL"
    ).fetchone()[0]
    print(f"Total rows to process: {total_rows:,}")

    # 4) Final, globally ordered scan
    query = f"""
        SELECT
            {key_expr} AS k,
            COALESCE(CAST(timestamp AS BIGINT), CAST(ts_ms AS BIGINT)) AS ts_ms,
            event_type, asset_id, bids, asks, changes, market, market_title, outcome
        FROM ev
        WHERE COALESCE(timestamp, ts_ms) IS NOT NULL
        ORDER BY k, ts_ms
    """

    # 5) Stream Arrow batches with a tqdm bar
    con.execute(query)
    with tqdm(total=total_rows, desc="Sorting & emitting", unit="events") as pbare:
        while True:
            batch = con.fetch_record_batch()  # requires: pip install pyarrow
            if batch is None or batch.num_rows == 0:
                break
            arrays = [batch.column(i) for i in range(len(batch.schema.names))]
            n = batch.num_rows
            for i in range(n):
                yield {
                    "k": arrays[0][i].as_py(),
                    "ts_ms": arrays[1][i].as_py(),
                    "event_type": arrays[2][i].as_py(),
                    "asset_id": arrays[3][i].as_py(),
                    "bids": arrays[4][i].as_py() or [],
                    "asks": arrays[5][i].as_py() or [],
                    "changes": arrays[6][i].as_py() or [],
                    "market": arrays[7][i].as_py() or "",
                    "market_title": arrays[8][i].as_py() or "",
                    "outcome": arrays[9][i].as_py() or "",
                }
            pbare.update(n)

def process_sorted_stream(sorted_iter, processor: L1Processor,
                          gap_threshold_s: float,
                          writer_main: Union[RotatingGzipWriter, str],
                          writer_outages: Optional[Union[RotatingGzipWriter, str]],
                          interleave_outages: bool,
                          verbose: bool = False):
    print("Processing DuckDB-sorted stream...")
    start_time = time.time()
    main_out_file = open(writer_main, "w", encoding="utf-8") if isinstance(writer_main, str) else None
    outages_out_file = open(writer_outages, "w", encoding="utf-8") if (writer_outages and isinstance(writer_outages, str)) else None

    def write_record(rec: Dict, is_outage: bool = False):
        j = dumps_text(rec)
        if is_outage and writer_outages and not interleave_outages:
            (outages_out_file.write(j + "\n") if outages_out_file else writer_outages.write(rec))
        else:
            (main_out_file.write(j + "\n") if main_out_file else writer_main.write(rec))

    prev_ts: Dict[str, int] = {}
    events_read = l1_updates = outages = 0
    ms = 1000.0

    for row in sorted_iter:
        k = row["k"]
        ts = int(row["ts_ms"])
        if k in prev_ts:
            gap_s = (ts - prev_ts[k]) / ms
            if gap_s > gap_threshold_s:
                outage_rec = {
                    "ts_ms": prev_ts[k],
                    "event_type": "no_network_event",
                    "gap_duration_ms": int(gap_s * 1000),
                    "outage_start": datetime.fromtimestamp(prev_ts[k]/1000.0, tz=timezone.utc).isoformat(),
                    "outage_end": datetime.fromtimestamp(ts/1000.0, tz=timezone.utc).isoformat(),
                    "stream_key": k,
                    "message": f"Network outage detected - no events for {gap_s:.3f} seconds",
                }
                write_record(outage_rec, is_outage=True)
                outages += 1
                if verbose:
                    print(f"OUTAGE {k}: {outage_rec['outage_start']} → {outage_rec['outage_end']} ({gap_s:.3f}s)")
        prev_ts[k] = ts

        ev = {
            "event_type": row.get("event_type"),
            "asset_id": row.get("asset_id"),
            "bids": row.get("bids") or [],
            "asks": row.get("asks") or [],
            "changes": row.get("changes") or [],
            "timestamp": ts,
            "ts_ms": ts,
            "market": row.get("market") or "",
            "market_title": row.get("market_title") or "",
            "outcome": row.get("outcome") or "",
        }

        events_read += 1
        l1 = processor.process_event(ev)
        if l1:
            write_record(l1.to_dict())
            l1_updates += 1

        if events_read % 100000 == 0:
            elapsed = time.time() - start_time
            print(f"Processed {events_read:,} events ({l1_updates:,} L1 updates, {outages} outages) | avg {events_read/elapsed:.0f}/s")

    if main_out_file: main_out_file.close()
    if outages_out_file: outages_out_file.close()
    print(f"Done: {events_read:,} events, {l1_updates:,} L1 updates, {outages} outages")
    return events_read, l1_updates, outages



# ---------------------------
# Main
# ---------------------------
def main():
    p = argparse.ArgumentParser(description="Preprocess L2 → L1 with DuckDB parallel sort")
    p.add_argument("input", help="Input file or directory path")
    p.add_argument("output", nargs="?", help="Output file (omit if using --cloud-output)")
    p.add_argument("--cloud-output", help="RotatingGzipWriter output dir")
    p.add_argument("--key-field", default="", help="Stream key field; default GLOBAL")
    p.add_argument("--gap-threshold-seconds", type=float, default=1.0, help="Gap threshold for outages")
    p.add_argument("--outages-output", help="Separate outages file (omit to interleave)")
    p.add_argument("--threads", type=int, default=0, help="DuckDB threads (0=all cores)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    if not args.cloud_output and not args.output:
        p.error("Must specify either output file or --cloud-output")
    if args.cloud_output and args.output:
        p.error("Cannot specify both output file and --cloud-output")

    key_field = args.key_field if args.key_field else None
    input_files = discover_input_files(args.input)
    print(f"Discovered {len(input_files)} input files")

    input_files = [os.path.abspath(p) for p in input_files if os.path.isfile(p)]
    input_files.sort()
    input_files = input_files[:-1]

    if args.cloud_output:
        os.makedirs(args.cloud_output, exist_ok=True)
        main_writer = RotatingGzipWriter(args.cloud_output)
        outages_writer = args.outages_output if args.outages_output else None
    else:
        main_writer = args.output
        outages_writer = args.outages_output if args.outages_output else None

    processor = L1Processor()
    interleave = args.outages_output is None
    sorted_iter = duckdb_sorted_row_iter(input_files, key_field, threads=args.threads)
    events_read, l1_updates, outages = process_sorted_stream(
        sorted_iter, processor, args.gap_threshold_seconds,
        main_writer, outages_writer, interleave, args.verbose
    )

    print("Batch complete!")
    print(f"Total events: {events_read}, L1 updates: {l1_updates}, Outages: {outages}")

if __name__ == "__main__":
    main()
