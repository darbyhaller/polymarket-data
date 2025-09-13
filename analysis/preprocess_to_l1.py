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
@dataclass(slots=True)
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
    files = sorted(os.path.abspath(p) for p in input_files if os.path.isfile(p))
    if not files:
        raise FileNotFoundError("No input files after filtering to real files.")
    print(f"Using {len(files)} files (first 3): {files[:3]}")

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='56GB'")
    con.execute("PRAGMA preserve_insertion_order=false")         # helps some sorts
    con.execute("PRAGMA enable_progress_bar=true")

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

    # Build one JSON reader over all paths (OPTIMIZATION 1: Kill per-file INSERTs)
    file_list = [q(p) for p in files]  # SQL-escape
    files_sql = ", ".join(f"'{p}'" for p in file_list)

    read_all_cte = f"""
    read_all AS (
        SELECT * FROM read_json(
            [{files_sql}],
            columns={COLUMNS_SPEC},
            format='newline_delimited'
        )
    )"""

    print(f"Reading all {len(files)} files in single scan...")

    key_expr = f"COALESCE(CAST({key_field} AS VARCHAR), 'GLOBAL')" if key_field else "'GLOBAL'"

    # Count rows once for a precise bar
    total_rows = con.execute(f"""
        WITH {read_all_cte}
        SELECT COUNT(*) FROM read_all WHERE COALESCE(timestamp, ts_ms) IS NOT NULL
    """).fetchone()[0]
    print(f"Total rows to process: {total_rows:,}")

    # Precompute top-of-book *in SQL* for book events; filter to meaningful rows only
    # - best bid = max price in bids[], with its size
    # - best ask = min price in asks[], with its size
    # We still pass 'changes' for price_change events (state updates happen in Python).
    query = f"""
    WITH {read_all_cte},
    base AS (
      SELECT
        {key_expr} AS k,
        COALESCE(CAST(timestamp AS BIGINT), CAST(ts_ms AS BIGINT)) AS ts_ms,
        event_type, asset_id, bids, asks, changes, market, market_title, outcome
      FROM read_all
      WHERE COALESCE(timestamp, ts_ms) IS NOT NULL
        AND (
             event_type = 'book'
          OR (event_type = 'price_change' AND json_array_length(changes) > 0)
        )
    ),
    b_best AS (
      SELECT
        k, ts_ms,
        FIRST(price) AS best_bid_price,
        FIRST(size)  AS best_bid_size
      FROM (
        SELECT b.k, b.ts_ms,
               CAST(json_extract(x.value, '$.price') AS DOUBLE) AS price,
               json_extract(x.value, '$.size')          AS size
        FROM base b,
             json_each(CASE WHEN b.event_type='book' THEN b.bids ELSE '[]' END) AS x
        ORDER BY b.k, b.ts_ms, price DESC NULLS LAST
      )
      GROUP BY k, ts_ms
    ),
    a_best AS (
      SELECT
        k, ts_ms,
        FIRST(price) AS best_ask_price,
        FIRST(size)  AS best_ask_size
      FROM (
        SELECT a.k, a.ts_ms,
               CAST(json_extract(x.value, '$.price') AS DOUBLE) AS price,
               json_extract(x.value, '$.size')          AS size
        FROM base a,
             json_each(CASE WHEN a.event_type='book' THEN a.asks ELSE '[]' END) AS x
        ORDER BY a.k, a.ts_ms, price ASC NULLS LAST
      )
      GROUP BY k, ts_ms
    )
    SELECT
      b.k,
      b.ts_ms,
      b.event_type,
      b.asset_id,
      b.market,
      b.market_title,
      b.outcome,
      bb.best_bid_price,
      bb.best_bid_size,
      aa.best_ask_price,
      aa.best_ask_size,
      b.changes
    FROM base b
    LEFT JOIN b_best bb ON bb.k=b.k AND bb.ts_ms=b.ts_ms
    LEFT JOIN a_best aa ON aa.k=b.k AND aa.ts_ms=b.ts_ms
    ORDER BY b.k, b.ts_ms
    """

    # Stream Arrow reader with a row-level tqdm
    con.execute(query)
    reader = con.fetch_record_batch()
    with tqdm(total=total_rows, desc="Sorting & processing", unit="events") as pbar:
        for batch in reader:  # pyarrow.RecordBatchReader
            n = batch.num_rows
            a = [batch.column(i) for i in range(len(batch.schema.names))]
            # yield tuples (cheaper than per-row dicts)
            for i in range(n):
                yield (
                    a[0][i].as_py(),             # k
                    a[1][i].as_py(),             # ts_ms
                    a[2][i].as_py(),             # event_type
                    a[3][i].as_py(),             # asset_id
                    a[4][i].as_py() or "",       # market
                    a[5][i].as_py() or "",       # market_title
                    a[6][i].as_py() or "",       # outcome
                    a[7][i].as_py(),             # best_bid_price (float or None)
                    a[8][i].as_py(),             # best_bid_size  (str or None)
                    a[9][i].as_py(),             # best_ask_price (float or None)
                    a[10][i].as_py(),            # best_ask_size  (str or None)
                    a[11][i].as_py() or [],      # changes (list for price_change)
                )
            pbar.update(n)

def process_sorted_stream(sorted_iter,
                          processor: L1Processor,
                          gap_threshold_s: float,
                          writer_main: Union[RotatingGzipWriter, str],
                          writer_outages: Optional[Union[RotatingGzipWriter, str]],
                          interleave_outages: bool,
                          verbose: bool = False):
    print("Processing DuckDB-sorted stream...")
    start_time = time.time()
    main_out_file = open(writer_main, "w", encoding="utf-8") if isinstance(writer_main, str) else None
    outages_out_file = open(writer_outages, "w", encoding="utf-8") if (writer_outages and isinstance(writer_outages, str)) else None

    def write_rec(d: Dict, is_outage: bool = False):
        line = dumps_text(d) + "\n"
        if is_outage and writer_outages and not interleave_outages:
            (outages_out_file.write(line) if outages_out_file else writer_outages.write(d))
        else:
            (main_out_file.write(line) if main_out_file else writer_main.write(d))

    prev_ts: Dict[str, int] = {}
    events_read = l1_updates = outages = 0
    ms = 1000.0

    for (k, ts, event_type, asset_id, market, market_title, outcome,
         best_bid_price, best_bid_size, best_ask_price, best_ask_size, changes) in sorted_iter:

        ts = int(ts)

        # Outage detection
        last = prev_ts.get(k)
        if last is not None:
            gap_s = (ts - last) / ms
            if gap_s > gap_threshold_s:
                outage = {
                    "ts_ms": last,
                    "event_type": "no_network_event",
                    "gap_duration_ms": int(gap_s * 1000),
                    "outage_start": datetime.fromtimestamp(last/1000.0, tz=timezone.utc).isoformat(),
                    "outage_end": datetime.fromtimestamp(ts/1000.0, tz=timezone.utc).isoformat(),
                    "stream_key": k,
                    "message": f"Network outage detected - no events for {gap_s:.3f} seconds",
                }
                write_rec(outage, is_outage=True)
                outages += 1
                if verbose:
                    print(f"OUTAGE {k}: {outage['outage_start']} → {outage['outage_end']} ({gap_s:.3f}s)")
        prev_ts[k] = ts

        events_read += 1

        if event_type == "book":
            # Build L1 quote directly from precomputed bests (no Python scan)
            q = L1Quote(
                ts_ms=ts,
                asset_id=asset_id,
                market=market,
                market_title=market_title,
                outcome=outcome,
                best_bid_u=p_to_u(best_bid_price) if best_bid_price is not None else None,
                best_ask_u=p_to_u(best_ask_price) if best_ask_price is not None else None,
                best_bid_sz=best_bid_size,
                best_ask_sz=best_ask_size,
            )
            processor.l1_state[asset_id] = q  # keep state for subsequent price_change events
            write_rec(q.to_dict())
            l1_updates += 1
        elif event_type == "price_change":
            # Use your existing state-based logic
            # Parse changes if it's a JSON string
            if isinstance(changes, str):
                try:
                    changes = json.loads(changes)
                except (json.JSONDecodeError, TypeError):
                    changes = []
            ev = {"event_type": "price_change",
                  "asset_id": asset_id,
                  "changes": changes,
                  "timestamp": ts,
                  "ts_ms": ts}
            l1 = processor.process_price_change(ev)
            if l1:
                write_rec(l1.to_dict())
                l1_updates += 1
        # else: nothing (we filtered in SQL already)

    if main_out_file: main_out_file.close()
    if outages_out_file: outages_out_file.close()
    elapsed = time.time() - start_time
    print(f"Done: {events_read:,} events, {l1_updates:,} L1 updates, {outages} outages "
          f"({events_read/max(1e-6,elapsed):.0f}/s)")
    return events_read, l1_updates, outages



def convert_to_parquet(input_files: List[str], output_parquet_path: str, threads: int = 0):
    """
    Convert JSON files to Parquet format for maximum performance.
    This is the biggest win: 2-5x speedup by doing JSON parsing once.
    """
    files = sorted(os.path.abspath(p) for p in input_files if os.path.isfile(p))
    if not files:
        raise FileNotFoundError("No input files after filtering to real files.")
    
    print(f"Converting {len(files)} JSON files to Parquet...")
    
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute("PRAGMA memory_limit='56GB'")
    con.execute("PRAGMA enable_progress_bar=true")

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

    # Build one JSON reader over all paths
    file_list = [q(p) for p in files]  # SQL-escape
    files_sql = ", ".join(f"'{p}'" for p in file_list)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)

    # One-time ingestion (parallel) - convert JSON to Parquet
    print("Converting JSON to Parquet (this may take a while but only happens once)...")
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_json([{files_sql}], columns={COLUMNS_SPEC}, format='newline_delimited')
        ) TO '{q(output_parquet_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    
    print(f"Parquet conversion complete: {output_parquet_path}")
    return output_parquet_path

def duckdb_sorted_row_iter_parquet(parquet_path: str, key_field: Optional[str], threads: int = 0):
    """
    Process data from Parquet file instead of JSON files.
    Much faster since JSON parsing is already done.
    """
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute("PRAGMA memory_limit='56GB'")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute("PRAGMA enable_progress_bar=true")

    def q(s: str) -> str:  # SQL-escape single quotes
        return s.replace("'", "''")

    key_expr = f"COALESCE(CAST({key_field} AS VARCHAR), 'GLOBAL')" if key_field else "'GLOBAL'"

    # Count rows once for a precise bar
    total_rows = con.execute(f"""
        SELECT COUNT(*) FROM '{q(parquet_path)}' WHERE COALESCE(timestamp, ts_ms) IS NOT NULL
    """).fetchone()[0]
    print(f"Total rows to process from Parquet: {total_rows:,}")

    # Run query against the Parquet file (much faster than JSON)
    query = f"""
    WITH base AS (
      SELECT
        {key_expr} AS k,
        COALESCE(CAST(timestamp AS BIGINT), CAST(ts_ms AS BIGINT)) AS ts_ms,
        event_type, asset_id, bids, asks, changes, market, market_title, outcome
      FROM '{q(parquet_path)}'
      WHERE COALESCE(timestamp, ts_ms) IS NOT NULL
        AND (
             event_type = 'book'
          OR (event_type = 'price_change' AND json_array_length(changes) > 0)
        )
    ),
    b_best AS (
      SELECT
        k, ts_ms,
        FIRST(price) AS best_bid_price,
        FIRST(size)  AS best_bid_size
      FROM (
        SELECT b.k, b.ts_ms,
               CAST(json_extract(x.value, '$.price') AS DOUBLE) AS price,
               json_extract(x.value, '$.size')          AS size
        FROM base b,
             json_each(CASE WHEN b.event_type='book' THEN b.bids ELSE '[]' END) AS x
        ORDER BY b.k, b.ts_ms, price DESC NULLS LAST
      )
      GROUP BY k, ts_ms
    ),
    a_best AS (
      SELECT
        k, ts_ms,
        FIRST(price) AS best_ask_price,
        FIRST(size)  AS best_ask_size
      FROM (
        SELECT a.k, a.ts_ms,
               CAST(json_extract(x.value, '$.price') AS DOUBLE) AS price,
               json_extract(x.value, '$.size')          AS size
        FROM base a,
             json_each(CASE WHEN a.event_type='book' THEN a.asks ELSE '[]' END) AS x
        ORDER BY a.k, a.ts_ms, price ASC NULLS LAST
      )
      GROUP BY k, ts_ms
    )
    SELECT
      b.k,
      b.ts_ms,
      b.event_type,
      b.asset_id,
      b.market,
      b.market_title,
      b.outcome,
      bb.best_bid_price,
      bb.best_bid_size,
      aa.best_ask_price,
      aa.best_ask_size,
      b.changes
    FROM base b
    LEFT JOIN b_best bb ON bb.k=b.k AND bb.ts_ms=b.ts_ms
    LEFT JOIN a_best aa ON aa.k=b.k AND aa.ts_ms=b.ts_ms
    ORDER BY b.k, b.ts_ms
    """

    # Stream Arrow reader with a row-level tqdm
    con.execute(query)
    reader = con.fetch_record_batch()
    with tqdm(total=total_rows, desc="Processing from Parquet", unit="events") as pbar:
        for batch in reader:  # pyarrow.RecordBatchReader
            n = batch.num_rows
            a = [batch.column(i) for i in range(len(batch.schema.names))]
            # yield tuples (cheaper than per-row dicts)
            for i in range(n):
                yield (
                    a[0][i].as_py(),             # k
                    a[1][i].as_py(),             # ts_ms
                    a[2][i].as_py(),             # event_type
                    a[3][i].as_py(),             # asset_id
                    a[4][i].as_py() or "",       # market
                    a[5][i].as_py() or "",       # market_title
                    a[6][i].as_py() or "",       # outcome
                    a[7][i].as_py(),             # best_bid_price (float or None)
                    a[8][i].as_py(),             # best_bid_size  (str or None)
                    a[9][i].as_py(),             # best_ask_price (float or None)
                    a[10][i].as_py(),            # best_ask_size  (str or None)
                    a[11][i].as_py() or [],      # changes (list for price_change)
                )
            pbar.update(n)

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
    
    # New optimization options
    p.add_argument("--use-parquet", action="store_true",
                   help="Convert JSON to Parquet first for maximum performance (2-5x speedup)")
    p.add_argument("--parquet-path", default="stage/events.parquet",
                   help="Path for Parquet staging file (default: stage/events.parquet)")
    p.add_argument("--parquet-only", action="store_true",
                   help="Only convert to Parquet, don't process L1 (useful for staging)")
    
    args = p.parse_args()

    if not args.cloud_output and not args.output and not args.parquet_only:
        p.error("Must specify either output file or --cloud-output (or use --parquet-only)")
    if args.cloud_output and args.output:
        p.error("Cannot specify both output file and --cloud-output")

    key_field = args.key_field if args.key_field else None
    
    # Check if input is already a Parquet file
    if os.path.isfile(args.input) and args.input.endswith('.parquet'):
        print(f"Detected Parquet input: {args.input}")
        if args.use_parquet or args.parquet_only:
            print("Warning: --use-parquet/--parquet-only ignored when input is already Parquet")
        
        # Process directly from Parquet
        sorted_iter = duckdb_sorted_row_iter_parquet(args.input, key_field, threads=args.threads)
        
    else:
        # JSON file workflow
        input_files = discover_input_files(args.input)
        print(f"Discovered {len(input_files)} input files")

        input_files = [os.path.abspath(p) for p in input_files if os.path.isfile(p)]
        input_files.sort()
        input_files = input_files[:-1]

        # Handle Parquet conversion workflow
        if args.use_parquet or args.parquet_only:
            parquet_path = convert_to_parquet(input_files, args.parquet_path, threads=args.threads)
            
            if args.parquet_only:
                print(f"Parquet conversion complete: {parquet_path}")
                print("Use this file with --input {parquet_path} (without --use-parquet) for fastest processing")
                return
            
            # Use Parquet for processing
            print("Processing from Parquet file (much faster than JSON)...")
            sorted_iter = duckdb_sorted_row_iter_parquet(parquet_path, key_field, threads=args.threads)
        else:
            # Use optimized JSON processing (single read_json call)
            print("Processing JSON files with optimized single-scan approach...")
            sorted_iter = duckdb_sorted_row_iter(input_files, key_field, threads=args.threads)

    # Set up writers
    if args.cloud_output:
        os.makedirs(args.cloud_output, exist_ok=True)
        main_writer = RotatingGzipWriter(args.cloud_output)
        outages_writer = args.outages_output if args.outages_output else None
    else:
        main_writer = args.output
        outages_writer = args.outages_output if args.outages_output else None

    processor = L1Processor()
    interleave = args.outages_output is None
    events_read, l1_updates, outages = process_sorted_stream(
        sorted_iter, processor, args.gap_threshold_seconds,
        main_writer, outages_writer, interleave, args.verbose
    )

    print("Batch complete!")
    print(f"Total events: {events_read}, L1 updates: {l1_updates}, Outages: {outages}")
    
    if args.use_parquet:
        print(f"\nTip: For subsequent runs, you can process the Parquet file directly:")
        print(f"  python {os.path.basename(__file__)} {args.parquet_path} [output] [other-args]")
        print(f"  (this skips JSON parsing and is much faster)")

if __name__ == "__main__":
    main()
