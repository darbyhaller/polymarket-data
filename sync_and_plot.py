#!/usr/bin/env python3
import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd
import matplotlib.pyplot as plt


def run(cmd):
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)


def parse_args():
    p = argparse.ArgumentParser(description="Sync Parquet from GCS and plot 10-minute event frequency.")
    p.add_argument("--gcs-prefix", required=True,
                   help="GCS prefix, e.g. gs://polymarket-raw-polymarket-470619/raw/")
    p.add_argument("--local-dir", default="./parquet_sync",
                   help="Local directory to sync into (default: ./parquet_sync)")
    p.add_argument("--event-types", nargs="*", default=[],
                   help="Filter to these event types (e.g. book price_change last_trade_price). Defaults to all.")
    p.add_argument("--start", type=str, default=None,
                   help="Start datetime (UTC) e.g. 2025-09-01T00:00:00")
    p.add_argument("--end", type=str, default=None,
                   help="End datetime (UTC, exclusive) e.g. 2025-09-08T00:00:00")
    p.add_argument("--bin", type=str, default="2H",
                   help="Bin size for plotting (e.g. 10min, 5min, 1H). Used for label aggregation only.")
    p.add_argument("--no-sync", action="store_true",
                   help="Skip gsutil sync (assume data already in local-dir).")
    return p.parse_args()


def to_utc_datetime(s):
    if s is None:
        return None
    # Accept "YYYY-MM-DD" or ISO8601
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(s, "%Y-%m-%d")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        raise ValueError(f"Unrecognized datetime: {s}")


def main():
    args = parse_args()
    local = Path(args.local_dir)
    local.mkdir(parents=True, exist_ok=True)

    if not args.no_sync:
        if args.event_types:
            # Sync only specified event_types
            for et in args.event_types:
                gcs_source = f"{args.gcs_prefix.rstrip('/')}/event_type={et}/"
                local_dest = local / f"event_type={et}"
                run(["gsutil", "-m", "rsync", "-r", gcs_source, str(local_dest)])
        else:
            # Full mirror of the GCS prefix into local dir
            run(["gsutil", "-m", "rsync", "-r", args.gcs_prefix, str(local)])

    # Open as a PyArrow dataset with hive partition discovery
    dataset = ds.dataset(str(local), format="parquet", partitioning="hive")

    # Build filter (pushdown): event_type + optional time window
    filt = None

    # Filter by event_type partition if provided
    if args.event_types:
        et_field = ds.field("event_type")
        et_filter = None
        for et in args.event_types:
            this = (et_field == et)
            et_filter = this if et_filter is None else (et_filter | this)
        filt = et_filter if filt is None else (filt & et_filter)

    # We’ll try to filter by row timestamp if present; else we fall back to partition time (year/month/day/hour)
    # Detect columns → prefer 'timestamp' (ms), else 'recv_ts_ms' (ms)
    # We won’t scan a full table; we just peek at schema from a fragment
    schema = dataset.schema
    has_ts = "timestamp" in schema.names
    has_recv = "recv_ts_ms" in schema.names

    # Time filters
    start_dt = to_utc_datetime(args.start)
    end_dt = to_utc_datetime(args.end)

    if start_dt or end_dt:
        if has_ts or has_recv:
            # Build filter on row-level time (best)
            col = ds.field("timestamp") if has_ts else ds.field("recv_ts_ms")
            # convert ms int to timestamp(ms) via compute later; here we compare as ms since epoch
            def to_ms(dt):
                return int(dt.timestamp() * 1000)

            if start_dt:
                f2 = (col >= to_ms(start_dt))
                filt = f2 if filt is None else (filt & f2)
            if end_dt:
                f3 = (col < to_ms(end_dt))
                filt = f3 if filt is None else (filt & f3)
        else:
            # Fall back to partition pruning (year/month/day/hour) — coarser but still narrows read
            def ymdh(dt):
                return dt.year, dt.month, dt.day, dt.hour
            y_field = ds.field("year")
            m_field = ds.field("month")
            d_field = ds.field("day")
            h_field = ds.field("hour")
            # Build a small OR-of-ANDs over hours in range
            # Caution: large ranges create big expressions; for “week-ish” it’s fine.
            if not (start_dt and end_dt):
                print("NOTE: Without row timestamps and only one bound, partition pruning may be weak.", file=sys.stderr)
            if not (start_dt and end_dt):
                # If only one bound, skip complex pruning; we’ll post-filter after scan.
                pass
            else:
                # Create discrete hourly buckets between start and end (exclusive of end)
                buckets = []
                cur = start_dt.replace(minute=0, second=0, microsecond=0)
                while cur < end_dt:
                    y, m, d, h = ymdh(cur)
                    buckets.append(((y_field == y) & (m_field == f"{m:02d}") & (d_field == f"{d:02d}") & (h_field == f"{h:02d}")))
                    cur = cur.replace(hour=cur.hour) + pd.Timedelta(hours=1)
                if buckets:
                    part_filter = buckets[0]
                    for b in buckets[1:]:
                        part_filter = part_filter | b
                    filt = part_filter if filt is None else (filt & part_filter)

    # Columns we need: event_type + time columns
    cols = [c for c in ["event_type", "timestamp", "recv_ts_ms", "year", "month", "day", "hour"] if c in schema.names]

    # Scan (Arrow, not pandas yet)
    scanner = dataset.scanner(filter=filt, columns=cols) if filt is not None else dataset.scanner(columns=cols)
    table = scanner.to_table()

    if table.num_rows == 0:
        print("No rows matched the filters. Nothing to plot.")
        return

    # Choose time column (ms), otherwise reconstruct from partition (hour precision only)
    ts_col = None
    if "timestamp" in table.column_names:
        ts_col = table.column("timestamp")
    elif "recv_ts_ms" in table.column_names:
        ts_col = table.column("recv_ts_ms")

    if ts_col is not None:
        # ts_col is int milliseconds since epoch → Arrow timestamp(ms, UTC)
        ts = pc.cast(ts_col, pa.timestamp("ms", tz="UTC"))
    else:
        # Reconstruct from partitions: year/month/day/hour (strings for hive)
        # Create a timestamp at the start of the hour (no within-hour fidelity)
        required = ["year", "month", "day", "hour"]
        if not all(c in table.column_names for c in required):
            raise RuntimeError("No usable timestamp in data and missing partition columns to reconstruct time.")
        year = pc.cast(table.column("year"), pa.int32())
        month = pc.cast(pc.utf8_to_number(table.column("month")), pa.int32())
        day = pc.cast(pc.utf8_to_number(table.column("day")), pa.int32())
        hour = pc.cast(pc.utf8_to_number(table.column("hour")), pa.int32())
        # Build an ISO string and parse to timestamp
        iso = pc.binary_join_element_wise(
            pc.binary_join_element_wise(pc.binary_join_element_wise(pc.cast(year, pa.string()), pc.scalar("-"), pc.utf8_slice_codeunits(pc.cast(pc.add(month, 100), pa.string()), 1, 2)),
                                        pc.scalar("-"),
                                        pc.utf8_slice_codeunits(pc.cast(pc.add(day, 100), pa.string()), 1, 2)),
            pc.scalar("T"),
            pc.utf8_slice_codeunits(pc.cast(pc.add(hour, 100), pa.string()), 1, 2)
        )
        iso = pc.binary_join_element_wise(iso, pc.scalar(":00:00Z"))  # at hour start
        ts = pc.strptime(iso, format="%Y-%m-%dT%H:%M:%SZ", unit="s", error_is_null=False)
        ts = pc.cast(ts, pa.timestamp("ms", tz="UTC"))

    # Add time column to table
    table = table.append_column("ts", ts)

    # Optional post time-window filter (if we could not push down on partitions)
    if start_dt:
        table = table.filter(pc.greater_equal(table["ts"], pa.scalar(start_dt, type=pa.timestamp("ms", tz="UTC"))))
    if end_dt:
        table = table.filter(pc.less(table["ts"], pa.scalar(end_dt, type=pa.timestamp("ms", tz="UTC"))))

    # Floor to 10-minute buckets (Arrow), group by bucket + event_type, count rows
    bucket = pc.floor_temporal(table["ts"], unit="minute", multiple=10)
    table = table.append_column("bucket_10m", bucket)

    group_keys = [("bucket_10m", "bucket_10m")]
    if "event_type" in table.column_names:
        group_keys.append(("event_type", "event_type"))
    grouped = table.group_by([k for _, k in group_keys]).aggregate([("ts", "count")])

    # Convert to pandas for plotting
    pdf = grouped.to_pandas()
    pdf["bucket_10m"] = pd.to_datetime(pdf["bucket_10m"], utc=True)

    # Pivot to wide: index = bucket, columns = event_type, values = count
    if "event_type" in pdf.columns:
        pivot = pdf.pivot_table(index="bucket_10m", columns="event_type", values="ts_count", aggfunc="sum").fillna(0)
    else:
        pivot = pdf.set_index("bucket_10m")[["ts_count"]]

    # Resample to requested bin for nicer labeling (defaults to 10min)
    pivot = pivot.sort_index().resample(args.bin).sum()

    # Plot
    ax = pivot.plot(figsize=(12, 5), linewidth=1.5)
    ax.set_title(f"Event frequency per {args.bin} — {'/'.join(args.event_types) if args.event_types else 'all event_types'}")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Events")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)
