#!/usr/bin/env python3
"""
Build L1 (top-of-book) data and detect trade outages from Polymarket parquet outputs.

Changes vs v1:
- Sort all processing by **timestamp** (exchange ts), not recv_ts_ms.
- L1 prices are **initialized from book events** (top-of-book).
- Detect **global outages**: periods where **no price_change events occur** within 1-second windows.
- Progress bars (tqdm) for scanning, deriving, writing, and outage detection. Suitable for ~2.5GB+.

Reads a parquet lake written by EventTypeParquetWriter with Hive partitions:
  event_type=.../year=YYYY/month=MM/day=DD/hour=HH

Outputs
- **Single** L1 parquet file (no partitioned folders), globally **sorted by ts_ms**.
- Optional CSV listing outages (start_ms,end_ms,duration_ms).

All monetary/size fields remain fixed-point integers:
- prices: uint32 (price * 10_000)
- sizes:  uint64 (size  * 10_000)

Usage:
  python analysis/l1.py \
      --input-root /var/data/polymarket/parquets \
      --output-root /var/data/polymarket/l1 \
      [--start "2025-09-05T00:00:00Z" --end "2025-09-07T00:00:00Z"] \
      [--batch-size 200_000] [--compression zstd] [--row-group-size 200_000] \
      [--outages-csv /var/data/polymarket/outages.csv] \
      [--outage-threshold-seconds 1.0]

Requirements: pyarrow>=12, tqdm
"""
import os
import sys
import argparse
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm


DEFAULTS = {
    "compression": os.getenv("PM_COMPRESSION", "zstd"),
    "batch_size": int(os.getenv("PM_BATCH_SIZE", "200_000")),
    "row_group_size": int(os.getenv("PM_ROW_GROUP_SIZE", "200_000")),
}


def parse_iso8601(ts: Optional[str]) -> Optional[int]:
    if not ts:
        return None
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def l1_schema() -> pa.Schema:
    return pa.schema([
        pa.field("recv_ts_ms", pa.int64()),
        pa.field("asset_id", pa.string()),
        pa.field("market", pa.string()),
        pa.field("market_title", pa.string()),
        pa.field("outcome", pa.string()),
        pa.field("ts_ms", pa.int64()),
        pa.field("bid_px", pa.uint32()),
        pa.field("bid_sz", pa.uint64()),
        pa.field("ask_px", pa.uint32()),
        pa.field("ask_sz", pa.uint64()),
        pa.field("mid_px", pa.uint32()),
        pa.field("spread_px", pa.uint32()),
        # Outage fields
        pa.field("is_outage", pa.bool_()),
        pa.field("outage_duration_ms", pa.int64()),
    ])


def derive_l1_batch_sorted_by_ts(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Compute L1 fields from book snapshots, **sorted by ts_ms** (exchange ts).

    Inputs: recv_ts_ms, asset_id, market, market_title, outcome, timestamp, bids, asks
    """
    # Convert to Python objects for easier processing
    bids_column = batch.column(batch.schema.get_field_index("bids"))
    asks_column = batch.column(batch.schema.get_field_index("asks"))
    
    # Process each row to extract best bid/ask
    num_rows = batch.num_rows
    bid_prices = []
    bid_sizes = []
    ask_prices = []
    ask_sizes = []
    
    for i in range(num_rows):
        bids = bids_column[i].as_py() if not bids_column[i].is_valid else []
        asks = asks_column[i].as_py() if not asks_column[i].is_valid else []
        
        # Handle bids
        if bids and len(bids) > 0:
            best_bid = bids[0]
            bid_prices.append(int(best_bid['price']))
            bid_sizes.append(int(best_bid['size']))
        else:
            bid_prices.append(None)
            bid_sizes.append(None)
            
        # Handle asks
        if asks and len(asks) > 0:
            best_ask = asks[0]
            ask_prices.append(int(best_ask['price']))
            ask_sizes.append(int(best_ask['size']))
        else:
            ask_prices.append(None)
            ask_sizes.append(None)
    
    # Convert back to PyArrow arrays
    bid_px = pa.array(bid_prices, type=pa.uint32())
    bid_sz = pa.array(bid_sizes, type=pa.uint64())
    ask_px = pa.array(ask_prices, type=pa.uint32())
    ask_sz = pa.array(ask_sizes, type=pa.uint64())
    
    # Calculate mid and spread
    mid_prices = []
    spreads = []
    for i in range(num_rows):
        bp = bid_prices[i]
        ap = ask_prices[i]
        if bp is not None and ap is not None:
            mid_prices.append((bp + ap) // 2)
            spreads.append(ap - bp)
        else:
            mid_prices.append(None)
            spreads.append(None)
    
    mid_px = pa.array(mid_prices, type=pa.uint32())
    spread_px = pa.array(spreads, type=pa.uint32())

    cols = [
        batch.column(batch.schema.get_field_index("recv_ts_ms")),
        batch.column(batch.schema.get_field_index("asset_id")),
        batch.column(batch.schema.get_field_index("market")),
        batch.column(batch.schema.get_field_index("market_title")),
        batch.column(batch.schema.get_field_index("outcome")),
        batch.column(batch.schema.get_field_index("timestamp")).cast(pa.int64()),
        bid_px,
        bid_sz,
        ask_px,
        ask_sz,
        mid_px,
        spread_px,
    ]
    out = pa.RecordBatch.from_arrays(cols, schema=l1_schema())

    # Sort by ts_ms (stable within the batch)
    sort_idx = pc.sort_indices(out.column(out.schema.get_field_index("ts_ms")))
    return out.take(sort_idx)


def write_single_parquet_sorted(batches: List[pa.RecordBatch], out_file: str,
                               compression: str, row_group_size: int):
    """Concatenate, globally sort by ts_ms, and write to a **single** parquet file.

    NOTE: This holds the concatenated table in memory to perform a true global sort.
    For ~2.5GB compressed inputs this is typically OK on modern machines; if needed
    we can spill/merge in a follow-up version.
    """
    ensure_dir(os.path.dirname(out_file) or ".")

    # Progress: concatenate
    total_rows = sum(b.num_rows for b in batches)
    pbar = tqdm(desc="Concatenating batches", unit="rows", total=total_rows)
    offset = 0
    table = pa.Table.from_batches(batches)
    pbar.update(total_rows)
    pbar.close()

    # Global sort by ts_ms
    ts_col = table.column(table.schema.get_field_index("ts_ms"))
    sort_idx = pc.sort_indices(ts_col)
    table_sorted = table.take(sort_idx)

    # Write single file with configured row group size
    with pq.ParquetWriter(
        out_file,
        schema=table_sorted.schema,
        compression=compression,
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
    ) as w:
        n = table_sorted.num_rows
        wrote = 0
        with tqdm(total=n, desc="Writing single parquet", unit="rows") as wp:
            start = 0
            while start < n:
                end = min(start + row_group_size, n)
                w.write_table(table_sorted.slice(start, end - start))
                wrote += (end - start)
                wp.update(end - start)
                start = end


def detect_outages(input_root: str, start_ms: Optional[int], end_ms: Optional[int],
                   threshold_ms: int) -> List[Tuple[int, int, int]]:
    """Detect periods with **no price_change events** within 1-second windows.

    Returns list of (start_ms, end_ms, duration_ms), sorted by start.
    """
    # Filter out .inprogress files
    dataset = ds.dataset(
        input_root,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True
    )
    # Additional filtering for .inprogress files
    valid_files = [f for f in dataset.files if not f.endswith('.inprogress')]
    if not valid_files:
        return []
    dataset = ds.dataset(valid_files, format="parquet", partitioning="hive")
    filt = (ds.field("event_type") == "price_change")
    if start_ms is not None:
        filt = filt & (ds.field("timestamp") >= start_ms)
    if end_ms is not None:
        filt = filt & (ds.field("timestamp") < end_ms)

    # Fast pre-count for a better progress bar total (optional)
    try:
        total_rows = dataset.count_rows(filter=filt)
    except Exception:
        total_rows = None

    scanner = dataset.scanner(
        columns=["timestamp"],
        filter=filt,
        use_threads=True,
        batch_size=DEFAULTS["batch_size"],
    )

    # Gather all price_change timestamps (int64 ms) then sort once.
    stamps: List[int] = []
    pbar = tqdm(desc="Scanning price_change events", unit="rows", total=total_rows)
    for b in scanner.to_batches():
        col = b.column(0).cast(pa.int64())
        # extend list efficiently
        for i in range(b.num_rows):
            v = col[i].as_py()
            if v is not None:
                stamps.append(v)
        if total_rows:
            pbar.update(b.num_rows)
    pbar.close()

    if not stamps:
        return []

    stamps.sort()
    print(f"Found {len(stamps)} price_change events")
    
    # Define 1-second window size (1000ms)
    window_ms = 1000
    
    # Start from the first price_change event, aligned to second boundaries
    first_ts = stamps[0]
    last_ts = stamps[-1]
    
    # Align start to second boundary
    start_window = (first_ts // window_ms) * window_ms
    end_window = ((last_ts // window_ms) + 1) * window_ms
    
    print(f"Checking 1-second windows from {start_window} to {end_window}")
    
    outages: List[Tuple[int, int, int]] = []
    current_outage_start = None
    
    # Efficiently iterate through windows using sorted timestamps
    window_start = start_window
    stamp_idx = 0
    total_windows = (end_window - start_window) // window_ms
    
    with tqdm(total=total_windows, desc="Checking 1s windows", unit="window") as pbar:
        while window_start < end_window:
            window_end = window_start + window_ms
            
            # Find if any timestamps fall in this window [window_start, window_end)
            has_events = False
            
            # Skip timestamps before this window
            while stamp_idx < len(stamps) and stamps[stamp_idx] < window_start:
                stamp_idx += 1
            
            # Check if any timestamp falls in current window
            if stamp_idx < len(stamps) and stamps[stamp_idx] < window_end:
                has_events = True
            
            if not has_events:
                # This is an outage window
                if current_outage_start is None:
                    current_outage_start = window_start
            else:
                # This window has events, close any ongoing outage
                if current_outage_start is not None:
                    outage_duration = window_start - current_outage_start
                    if outage_duration >= threshold_ms:
                        outages.append((current_outage_start, window_start, outage_duration))
                    current_outage_start = None
            
            window_start = window_end
            pbar.update(1)
    
    # Handle final outage if still ongoing
    if current_outage_start is not None:
        outage_duration = end_window - current_outage_start
        if outage_duration >= threshold_ms:
            outages.append((current_outage_start, end_window, outage_duration))
    
    print(f"Detected {len(outages)} outage periods >= {threshold_ms}ms")
    return outages


def main():
    ap = argparse.ArgumentParser(description="Build L1 (top-of-book) and detect trade outages")
    ap.add_argument("--input-root", required=True, help="Root of source parquet lake (contains event_type=...)")
    ap.add_argument("--output-file", required=True, help="Path to write SINGLE L1 parquet file (no partitioning)")
    ap.add_argument("--start", help="ISO8601 start (inclusive), e.g. 2025-09-05T00:00:00Z")
    ap.add_argument("--end", help="ISO8601 end (exclusive), e.g. 2025-09-06T00:00:00Z")
    ap.add_argument("--batch-size", type=int, default=DEFAULTS["batch_size"], help="Scanner batch size")
    ap.add_argument("--row-group-size", type=int, default=DEFAULTS["row_group_size"], help="Output row group size")
    ap.add_argument("--compression", default=DEFAULTS["compression"], help="Output parquet compression (zstd, snappy, gzip)")
    ap.add_argument("--outages-csv", help="Optional path to write CSV of detected outages")
    ap.add_argument("--outage-threshold-seconds", type=float, default=1.0, help="Minimum outage duration in seconds to report (windows without price_change events)")
    args = ap.parse_args()

    start_ms = parse_iso8601(args.start)
    end_ms = parse_iso8601(args.end)

    # ==== OUTAGE DETECTION FIRST (from price_change events) ====
    print("=== DETECTING OUTAGES FIRST ===")
    threshold_ms = int(args.outage_threshold_seconds * 1000)
    outages = detect_outages(args.input_root, start_ms, end_ms, threshold_ms)

    if outages:
        print(f"Detected {len(outages)} outage(s) > {threshold_ms} ms")
        if args.outages_csv:
            ensure_dir(os.path.dirname(args.outages_csv))
            import csv
            with open(args.outages_csv, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["start_ms","end_ms","duration_ms"])  # inclusive/exclusive boundary
                for s,e,d in tqdm(outages, desc="Writing outages CSV", unit="outage"):
                    w.writerow([s,e,d])
            print(f"Outages written to {args.outages_csv}")
        
        # Also write outages to a default file immediately
        outages_file = args.output_file.replace('.parquet', '_outages.csv')
        outages_dir = os.path.dirname(outages_file)
        if outages_dir:  # Only create directory if there is one
            ensure_dir(outages_dir)
        import csv
        with open(outages_file, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["start_ms","end_ms","duration_ms"])
            for s,e,d in outages:
                w.writerow([s,e,d])
        print(f"Outages also written to {outages_file}")
    else:
        print("No outages detected for given window/threshold.")

    # ==== L1 BUILD (from book AND price_change events), sorted by exchange timestamp ====
    # Filter out .inprogress files
    temp_dataset = ds.dataset(
        args.input_root,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True
    )
    # Additional filtering for .inprogress files
    valid_files = [f for f in temp_dataset.files if not f.endswith('.inprogress')]
    if not valid_files:
        print("No valid parquet files found (all are .inprogress)")
        return
    dataset = ds.dataset(valid_files, format="parquet", partitioning="hive")
    
    # Filter for BOTH book and price_change events
    filt = (ds.field("event_type") == "book") | (ds.field("event_type") == "price_change")
    if start_ms is not None:
        filt = filt & (ds.field("timestamp") >= start_ms)
    if end_ms is not None:
        filt = filt & (ds.field("timestamp") < end_ms)

    columns = [
        "recv_ts_ms",
        "asset_id",
        "market",
        "market_title",
        "outcome",
        "timestamp",
        "event_type",
        "bids",
        "asks",
    ]

    # Process both book and price_change events to build L1
    try:
        total_rows = dataset.count_rows(filter=filt)
    except Exception:
        total_rows = None

    scanner = dataset.scanner(
        columns=columns,
        filter=filt,
        use_threads=True,
        batch_size=args.batch_size,
    )

    print("Processing both book and price_change events...")
    # We need to process events chronologically to maintain proper L1 state
    # This is more complex - we need to collect all events, sort globally, then process
    all_events = []
    
    pbar = tqdm(desc="Collecting events", unit="rows", total=total_rows)
    for batch in scanner.to_batches():
        for i in range(batch.num_rows):
            event = {
                'recv_ts_ms': batch.column(0)[i].as_py(),
                'asset_id': batch.column(1)[i].as_py(),
                'market': batch.column(2)[i].as_py(),
                'market_title': batch.column(3)[i].as_py(),
                'outcome': batch.column(4)[i].as_py(),
                'timestamp': batch.column(5)[i].as_py(),
                'event_type': batch.column(6)[i].as_py(),
                'bids': batch.column(7)[i].as_py() if batch.column(7)[i].is_valid else None,
                'asks': batch.column(8)[i].as_py() if batch.column(8)[i].is_valid else None,
            }
            all_events.append(event)
        pbar.update(batch.num_rows)
    pbar.close()

    if not all_events:
        print("No events found for the specified time range")
        return

    # Sort all events by timestamp
    print("Sorting events by timestamp...")
    all_events.sort(key=lambda x: x['timestamp'])
    
    # Build L1 records by maintaining state per asset
    print("Building L1 records...")
    l1_records = []
    asset_states = {}  # asset_id -> current L1 state
    
    for event in tqdm(all_events, desc="Processing events"):
        asset_id = event['asset_id']
        
        if event['event_type'] == 'book':
            # Book event - update full book state
            bids = event['bids']
            asks = event['asks']
            
            bid_px = bid_sz = ask_px = ask_sz = None
            if bids and len(bids) > 0:
                bid_px = int(bids[0]['price'])
                bid_sz = int(bids[0]['size'])
            if asks and len(asks) > 0:
                ask_px = int(asks[0]['price'])
                ask_sz = int(asks[0]['size'])
                
        elif event['event_type'] == 'price_change':
            # Price change event - process bids/asks like book events
            # (price_change events seem to have the same structure as book events)
            bids = event['bids']
            asks = event['asks']
            
            bid_px = bid_sz = ask_px = ask_sz = None
            if bids and len(bids) > 0:
                bid_px = int(bids[0]['price'])
                bid_sz = int(bids[0]['size'])
            if asks and len(asks) > 0:
                ask_px = int(asks[0]['price'])
                ask_sz = int(asks[0]['size'])
        else:
            continue
            
        # Calculate mid and spread
        mid_px = spread_px = None
        if bid_px is not None and ask_px is not None:
            mid_px = (bid_px + ask_px) // 2
            spread_px = ask_px - bid_px
            
        # Update asset state
        asset_states[asset_id] = (bid_px, bid_sz, ask_px, ask_sz)
        
        # Create L1 record
        l1_record = {
            'recv_ts_ms': event['recv_ts_ms'],
            'asset_id': asset_id,
            'market': event['market'],
            'market_title': event['market_title'],
            'outcome': event['outcome'],
            'ts_ms': event['timestamp'],
            'bid_px': bid_px,
            'bid_sz': bid_sz,
            'ask_px': ask_px,
            'ask_sz': ask_sz,
            'mid_px': mid_px,
            'spread_px': spread_px,
        }
        l1_records.append(l1_record)
    
    print(f"Generated {len(l1_records)} L1 records")
    
    # Convert to PyArrow table and write
    if l1_records:
        l1_table = pa.Table.from_pylist(l1_records, schema=l1_schema())
        print(f"Writing L1 data to {args.output_file}")
        ensure_dir(os.path.dirname(args.output_file) or ".")
        pq.write_table(l1_table, args.output_file, compression=args.compression)
        print(f"L1 data written to {args.output_file}")



if __name__ == "__main__":
    main()
