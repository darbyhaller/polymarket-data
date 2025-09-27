#!/usr/bin/env python3
import os, re, glob, argparse
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

# Partitioned like: parquets/event_type=.../year=YYYY/month=MM/day=DD/hour=HH/events-*.parquet
ROW_RE = re.compile(r".*year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)/events-\d+\.parquet$")
TIMESTAMP_COL = "timestamp"  # ms since epoch

def to_utc_ms(iso):
    ts = pd.Timestamp(iso)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return int(ts.value // 10**6), ts

def hours_in_range(start_ts, end_ts):
    start_hour = start_ts.floor("H")
    end_hour = (end_ts - pd.Timedelta(nanoseconds=1)).floor("H")
    return pd.date_range(start_hour, end_hour, freq="H", tz="UTC")

def gather_files(root, event_type, hours):
    pats = [os.path.join(
        root,
        f"event_type={event_type}",
        f"year={h.year:04d}",
        f"month={h.month:02d}",
        f"day={h.day:02d}",
        f"hour={h.hour:02d}",
        "events-*.parquet",
    ) for h in hours]
    files = []
    for p in pats:
        files.extend(glob.glob(p))
    return sorted(set(files))

def main():
    ap = argparse.ArgumentParser(description="Print all events in a small UTC window.")
    ap.add_argument("--root", default="parquets")
    ap.add_argument("--event-type", default="order_update_SELL")
    ap.add_argument("--start", default="2025-09-26T08:31:07Z", help="inclusive (UTC)")
    ap.add_argument("--end",   default="2025-09-26T08:31:09Z", help="exclusive (UTC)")
    ap.add_argument("--columns", nargs="*", help="Optional extra columns to include; timestamp always included")
    args = ap.parse_args()

    start_ms, start_ts = to_utc_ms(args.start)
    end_ms,   end_ts   = to_utc_ms(args.end)
    if not (end_ms > start_ms):
        raise SystemExit("End must be greater than start.")

    hours = hours_in_range(start_ts, end_ts)
    files = gather_files(args.root, args.event_type, hours)
    if not files:
        raise SystemExit("No matching parquet shards found for that window.")

    filt = (ds.field(TIMESTAMP_COL) >= pa.scalar(start_ms, pa.int64())) & \
           (ds.field(TIMESTAMP_COL) <  pa.scalar(end_ms, pa.int64()))
    cols = None
    if args.columns:
        cols = list(dict.fromkeys([TIMESTAMP_COL] + args.columns))

    dataset = ds.dataset(files, format="parquet")
    table = dataset.to_table(filter=filt, columns=cols)
    if table.num_rows == 0:
        print("No rows in that window.")
        return

    df = table.to_pandas()
    df[TIMESTAMP_COL] = pd.to_numeric(df[TIMESTAMP_COL], errors="coerce").astype("int64")
    df["timestamp_dt"] = pd.to_datetime(df[TIMESTAMP_COL], unit="ms", utc=True)
    df.sort_values(TIMESTAMP_COL, inplace=True)

    # Print all rows (pretty)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    print(df.to_string(index=False))

    # Small footer
    print(f"\nRows: {len(df)}  |  Time range: {df['timestamp_dt'].min()} → {df['timestamp_dt'].max()} (UTC)")
    print(f"Window was: {start_ts.isoformat()} to {end_ts.isoformat()} (UTC, end exclusive)")

if __name__ == "__main__":
    main()
