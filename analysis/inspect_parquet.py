#!/usr/bin/env python3
"""
Simple script to inspect rows from a parquet file.
"""

import sys
import pyarrow.parquet as pq
import pyarrow as pa
import json
import numpy as np


def get_schema_for_event_type(event_type: str) -> pa.Schema:
    """Get the appropriate schema for the given event type."""
    # Common base fields
    base_fields = [
        pa.field("recv_ts_ms", pa.int64()),
        pa.field("event_type", pa.string()),
        pa.field("asset_id", pa.string()),
        pa.field("market", pa.string()),
        pa.field("market_title", pa.string()),
        pa.field("outcome", pa.string()),
        pa.field("timestamp", pa.int64()),
    ]
    
    # Order summary schema for bids/asks
    order_summary_schema = pa.struct([
        pa.field("price", pa.uint32()),
        pa.field("size", pa.uint64())
    ])
    
    # Price change schema
    price_change_schema = pa.struct([
        pa.field("price", pa.uint32()),
        pa.field("side", pa.string()),
        pa.field("size", pa.uint64())
    ])
    
    if event_type == "last_trade_price":
        return pa.schema(base_fields + [
            pa.field("price", pa.uint32()),
            pa.field("size", pa.uint64()),
            pa.field("side", pa.string()),
            pa.field("fee_rate_bps", pa.uint32())
        ])
    elif event_type == "price_change":
        return pa.schema(base_fields + [
            pa.field("hash", pa.string()),
            pa.field("changes", pa.list_(price_change_schema))
        ])
    elif event_type == "book":
        return pa.schema(base_fields + [
            pa.field("hash", pa.string()),
            pa.field("bids", pa.list_(order_summary_schema)),
            pa.field("asks", pa.list_(order_summary_schema))
        ])
    elif event_type == "tick_size_change":
        return pa.schema(base_fields + [
            pa.field("old_tick_size", pa.uint32()),
            pa.field("new_tick_size", pa.uint32())
        ])
    else:
        raise ValueError(f"Unknown event type: {event_type}")


def detect_event_type(file_path: str) -> str:
    """Detect event type from file path."""
    if "event_type=last_trade_price" in file_path:
        return "last_trade_price"
    elif "event_type=price_change" in file_path:
        return "price_change"
    elif "event_type=book" in file_path:
        return "book"
    elif "event_type=tick_size_change" in file_path:
        return "tick_size_change"
    else:
        raise ValueError(f"Cannot detect event type from path: {file_path}")


def inspect_parquet_file(file_path: str, num_rows: int = 10, pretty: bool = True) -> None:
    """Inspect the first N rows of a parquet file."""
    # Detect event type and get appropriate schema
    event_type = detect_event_type(file_path)
    schema = get_schema_for_event_type(event_type)
    
    print(f"Detected event type: {event_type}")
    
    # Read the table with the detected schema
    table = pq.read_table(file_path, schema=schema)
    
    print(f"File: {file_path}")
    print(f"Total rows: {table.num_rows:,}")
    print(f"Total columns: {len(table.schema)}")
    print()
    
    # Get first N rows
    if table.num_rows == 0:
        print("No data rows found.")
        return
        
    # Convert to Python objects
    print(table[:10])
    rows = table.column('timestamp').to_pylist()
    rows = np.array(sorted(rows))
    gaps = np.diff(rows)/1000
    idxs = np.where(gaps > 1)[0]
    print(len(idxs))
    print(idxs/len(gaps))
    print(gaps[idxs])
    print(np.diff(rows[idxs])/1000)
    print(sum(gaps[idxs])/60)
    print((rows[-1]-rows[0])/60/1000)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Inspect parquet file rows")
    parser.add_argument("file_path", help="Path to parquet file")
    parser.add_argument("-n", "--num-rows", type=int, default=10, help="Number of rows to show (default: 10)")
    parser.add_argument("--no-pretty", action="store_true", help="Don't pretty print JSON")
    args = parser.parse_args()
    
    inspect_parquet_file(args.file_path, args.num_rows, not args.no_pretty)


if __name__ == "__main__":
    main()