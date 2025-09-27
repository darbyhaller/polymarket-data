#!/usr/bin/env python3
"""
visualize_window.py
- Extract a precise time window from partitioned parquet shards (pyarrow.dataset pushdown).
- Plot an interactive, readable chart:
    Row 1: Events per second (bar)
    Row 2: Inter-event gap in seconds over time (line, right-edge)
Usage (zsh):
  python visualize_window.py --root parquets --event-type order_update_SELL \
    --start 2025-09-26T08:30:00Z --end 2025-09-26T08:32:00Z
"""

import os, re, glob, argparse
import numpy as np
import pandas as pd

import pyarrow as pa
import pyarrow.dataset as ds

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# ------------ Defaults ------------
DEFAULT_ROOT = "parquets"
DEFAULT_EVENT_TYPE = "order_update_SELL"
DEFAULT_START = "2025-09-26T08:30:00Z"
DEFAULT_END   = "2025-09-26T08:32:00Z"
TIMESTAMP_COL = "timestamp"   # ms since epoch (int64)
HTML_OUT      = "window_view.html"
# ----------------------------------

pio.renderers.default = "browser"  # open in default browser on mac

row_re = re.compile(r".*year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)/events-\d+\.parquet$")

def parse_args():
    ap = argparse.ArgumentParser(description="Extract and visualize a precise time window from parquet shards.")
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--event-type", default=DEFAULT_EVENT_TYPE)
    ap.add_argument("--start", default=DEFAULT_START, help="ISO8601 UTC, e.g., 2025-09-26T08:30:00Z")
    ap.add_argument("--end",   default=DEFAULT_END,   help="ISO8601 UTC, e.g., 2025-09-26T08:32:00Z (exclusive)")
    ap.add_argument("--columns", nargs="*", help="Optional additional columns to include (timestamp is always included).")
    ap.add_argument("--out-html", default=HTML_OUT)
    return ap.parse_args()

def to_utc_ms(iso):
    ts = pd.Timestamp(iso)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value // 10**6), ts

def hours_in_range(start_ts, end_ts):
    start_hour = start_ts.floor("H")
    end_hour = (end_ts - pd.Timedelta(nanoseconds=1)).floor("H")
    hours = pd.date_range(start_hour, end_hour, freq="H", tz="UTC")
    return [(h.year, h.month, h.day, h.hour) for h in hours]

def gather_files(root, event_type, hours):
    pats = []
    for (y, m, d, h) in hours:
        pats.append(os.path.join(
            root,
            f"event_type={event_type}",
            f"year={y:04d}",
            f"month={m:02d}",
            f"day={d:02d}",
            f"hour={h:02d}",
            "events-*.parquet",
        ))
    files = []
    for p in pats:
        files.extend(glob.glob(p))
    return sorted(set(files))

def load_window_df(files, start_ms, end_ms, extra_cols=None):
    # Build predicate and projection
    filt = (ds.field(TIMESTAMP_COL) >= pa.scalar(start_ms, pa.int64())) & \
           (ds.field(TIMESTAMP_COL) <  pa.scalar(end_ms, pa.int64()))
    cols = None
    if extra_cols:
        cols = list(dict.fromkeys([TIMESTAMP_COL] + extra_cols))
    dataset = ds.dataset(files, format="parquet")
    table = dataset.to_table(filter=filt, columns=cols)
    if table.num_rows == 0:
        return pd.DataFrame(columns=cols or None)
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    # Ensure numeric timestamp dtype
    df[TIMESTAMP_COL] = pd.to_numeric(df[TIMESTAMP_COL], errors="coerce").astype("int64")
    df.sort_values(TIMESTAMP_COL, inplace=True)
    df["timestamp_dt"] = pd.to_datetime(df[TIMESTAMP_COL], unit="ms", utc=True)
    return df

def compute_gaps(ts_ms):
    """Return (gaps_sec, right_edge_times) where right_edge_times are pandas Timestamps (UTC)."""
    if ts_ms.size < 2:
        return np.array([]), pd.to_datetime([], utc=True)
    gaps_sec = np.diff(ts_ms / 1000.0)
    right_edge = pd.to_datetime(ts_ms[1:], unit="ms", utc=True)
    return gaps_sec, right_edge

def plot_window(df, start_ts, end_ts, out_html, event_type):
    # Aggregate events per second
    counts = (
        df.set_index("timestamp_dt")
          .assign(one=1)
          .resample("1S")["one"].sum()
          .reindex(pd.date_range(start_ts.floor("S"), end_ts.ceil("S"), freq="1S", tz="UTC"), fill_value=0)
    )

    # Gaps line
    gaps_sec, right_edge = compute_gaps(df[TIMESTAMP_COL].to_numpy())
    gap_series = pd.Series(gaps_sec, index=right_edge)

    # Build figure with two rows, shared x
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.35, 0.65],
        vertical_spacing=0.06,
        specs=[[{"type": "bar"}],[{"type": "scatter"}]]
    )

    # Row 1: events per second
    fig.add_bar(
        x=counts.index, y=counts.values, name="Events/sec",
        hovertemplate="Second: %{x|%Y-%m-%d %H:%M:%S}<br>count: %{y}<extra></extra>",
        marker_line_width=0, row=1, col=1
    )

    # Row 2: gap line (right-edge assignment)
    fig.add_scatter(
        x=gap_series.index, y=gap_series.values, mode="lines+markers",
        name="Inter-event gap (s, right edge)",
        hovertemplate="Time: %{x|%Y-%m-%d %H:%M:%S.%f}<br>gap: %{y:.6g}s<extra></extra>",
        row=2, col=1
    )

    # Window shading (visual cue)
    fig.add_vrect(x0=start_ts, x1=end_ts, fillcolor="LightGray", opacity=0.15, layer="below", line_width=0, row="all", col=1)

    # Layout
    fig.update_layout(
        title=f"Events and Inter-event Gaps — {event_type}  |  {start_ts.isoformat()} to {end_ts.isoformat()} (UTC)",
        hovermode="x unified",
        margin=dict(l=60, r=20, t=70, b=40),
        showlegend=True,
    )
    fig.update_xaxes(title="Time (UTC)", row=2, col=1)
    fig.update_yaxes(title="Events / sec", rangemode="tozero", row=1, col=1)
    fig.update_yaxes(title="Gap (seconds)", rangemode="tozero", row=2, col=1)

    fig.show()
    fig.write_html(out_html, include_plotlyjs="cdn", full_html=True)
    print(f"Wrote {out_html}")

def main():
    args = parse_args()

    start_ms, start_ts = to_utc_ms(args.start)
    end_ms,   end_ts   = to_utc_ms(args.end)
    if not (end_ms > start_ms):
        raise SystemExit("End must be greater than start.")

    # Determine which hour partitions to scan
    hours = hours_in_range(start_ts, end_ts)
    files = gather_files(args.root, args.event_type, hours)
    if not files:
        raise SystemExit(f"No parquet files in requested window under {args.root}")

    print(f"Scanning {len(files)} shard(s) across {len(hours)} hour partition(s)…")
    print(f"Window: {start_ts.isoformat()}  to  {end_ts.isoformat()}  (UTC)")

    df = load_window_df(files, start_ms, end_ms, args.columns)
    if df.empty:
        print("No rows found in that window.")
        return

    print(f"Loaded {len(df)} rows. Time span in data: {df['timestamp_dt'].min()} → {df['timestamp_dt'].max()}")

    plot_window(df, start_ts, end_ts, args.out_html, args.event_type)

if __name__ == "__main__":
    main()
