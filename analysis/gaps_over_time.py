#!/usr/bin/env python3
import os, re, glob
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from plotly.colors import qualitative as pq

# ========= Config (tweak these) =========
# Map legend label -> base path
roots = [
    "us-central1", "us-east1", "europe-west4", "africa-south1", "asia-northeast1", "asia-southeast1", "australia-southeast1", "me-west1", "southamerica-east1"
]
ROOTS = {root: "all-parquets/" + root + "-parquets/parquets" for root in roots}

EVENT_TYPE   = "price_change_BUY"
DAY_FILTER   = "05"   # "*" for all
HOUR_FILTER  = "*"   # "*" for all

TIMESTAMP_COL  = "timestamp"         # ms since epoch
DELAY_COL      = "delay"             # ms; recv_ms - server timestamp
USE_RIGHT_EDGE = True
RESAMPLE       = "1M"
PCTL           = 0.999999
HTML_OUT       = "per_minute_pctl_gap_multi.html"
OPEN_BROWSER   = True
pio.renderers.default = "browser" if OPEN_BROWSER else "notebook_connected"
# =======================================

row_re = re.compile(r".*year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)/events-\d+\.parquet$")

def load_series_for_root(root):
    pattern = os.path.join(
        root, f"{EVENT_TYPE}",
        "year=*","month=*",
        f"day={DAY_FILTER}", f"hour={HOUR_FILTER}",
        "events-*.parquet",
    )
    files = sorted(glob.glob(pattern))
    if not files:
        raise RuntimeError(f"No parquet files matched: {pattern}")
    m = row_re.match(files[0])
    if not m:
        raise RuntimeError(f"Could not parse time from filename: {files[0]}")
    y, mth, d, h = map(int, m.groups())
    file_start = pd.Timestamp(y, mth, d, h, tz="UTC")

    all_ts = []
    delay_points_time = []
    delay_points_vals = []

    for f in files:
        df = pd.read_parquet(f, columns=[TIMESTAMP_COL, DELAY_COL])
        ts = pd.to_numeric(df[TIMESTAMP_COL], errors="coerce").dropna().astype("int64").to_numpy()
        if ts.size:
            all_ts.append(ts)
            # delay aligned to event timestamp
            if DELAY_COL in df.columns:
                delays = pd.to_numeric(df[DELAY_COL], errors="coerce").to_numpy()
                # filter to same valid rows we kept for ts
                valid = ~np.isnan(delays)
                if valid.any():
                    tsv = ts[valid]
                    dv = delays[valid]
                    delay_points_time.append(tsv)
                    delay_points_vals.append(dv)

    if not all_ts:
        raise RuntimeError(f"No timestamps found in matched files for {root}")

    ts = np.sort(np.concatenate(all_ts))
    ts = ts[ts >= int(file_start.value // 10**6)]
    if ts.size < 2:
        raise RuntimeError(f"Fewer than 2 timestamps after clamping for {root}")

    # pctl gaps per RESAMPLE (seconds)
    gaps_sec = np.diff(ts / 1000.0)
    edge = ts[1:] if USE_RIGHT_EDGE else ts[:-1]
    gap_time = pd.to_datetime(edge, unit="ms", utc=True)
    s_gap = pd.Series(gaps_sec, index=gap_time).resample(RESAMPLE).quantile(PCTL).dropna()

    # average delay (ms) per RESAMPLE, aligned to event time
    if delay_points_time and delay_points_vals:
        t = pd.to_datetime(np.concatenate(delay_points_time), unit="ms", utc=True)
        v = np.concatenate(delay_points_vals).astype("float64")
        s_delay = pd.Series(v, index=t).resample(RESAMPLE).mean().dropna()
    else:
        s_delay = pd.Series(dtype="float64")

    return s_gap, ts, s_delay

# ---- per-region series + raw timestamps (generic) ----
series_by_region = {}
delay_by_region = {}
ts_by_region = {}

for label, path in ROOTS.items():
    s_gap, ts, s_delay = load_series_for_root(path)
    series_by_region[label] = s_gap
    delay_by_region[label] = s_delay
    ts_by_region[label] = ts

# ---- union across all regions ----
# gaps union
all_ts = np.sort(np.concatenate(list(ts_by_region.values())))
gaps_union = np.diff(all_ts / 1000.0)
edge_union = all_ts[1:] if USE_RIGHT_EDGE else all_ts[:-1]
t_union = pd.to_datetime(edge_union, unit="ms", utc=True)
s_union = pd.Series(gaps_union, index=t_union).resample(RESAMPLE).quantile(PCTL).dropna()

# delay union (concatenate raw delay points from each region, then resample mean)
delay_union_raw = []
delay_union_time = []
for s in delay_by_region.values():
    if s is not None and not s.empty:
        # use the underlying events rather than resampled means to form a true union
        # we can't recover per-event values from resampled series, so take as-is if only resampled data is available
        # here we approximate union by reusing the resampled means (best effort) if needed
        # Prefer raw: if the series has a .index.freq set, it's resampled; but we didn't keep raw,
        # so just combine resampled series (acceptable for visualization).
        delay_union_raw.append(s)
if delay_union_raw:
    # Align and average across regions
    delay_union_df = pd.concat(delay_union_raw, axis=1).mean(axis=1)
    s_union_delay = delay_union_df.resample(RESAMPLE).mean().dropna()
else:
    s_union_delay = pd.Series(dtype="float64")

# ---- plotting ----
palette = (pq.Set1 + pq.Set2 + pq.Set3 + pq.Dark24 + pq.Light24)
title = f"p{int(PCTL*100)} gaps per {RESAMPLE} — {EVENT_TYPE}"

fig = make_subplots(specs=[[{"secondary_y": True}]])

# Gaps (primary y, log)
for i, (label, s) in enumerate(series_by_region.items()):
    color = palette[i % len(palette)]
    fig.add_trace(
        go.Scatter(x=s.index, y=s.values, mode="lines", name=f"{label} gap p{int(PCTL*100)}",
                   line=dict(color=color)),
        secondary_y=False
    )

fig.add_trace(
    go.Scatter(x=s_union.index, y=s_union.values, mode="lines", name="union gap",
               line=dict(width=3)),
    secondary_y=False
)

# Average delay (secondary y, ms)
for i, (label, s) in enumerate(delay_by_region.items()):
    if s is None or s.empty:
        continue
    color = palette[i % len(palette)]
    fig.add_trace(
        go.Scatter(x=s.index, y=s.values, mode="lines", name=f"{label} avg delay (ms)",
                   line=dict(color=color, dash="dot")),
        secondary_y=True
    )

if not s_union_delay.empty:
    fig.add_trace(
        go.Scatter(x=s_union_delay.index, y=s_union_delay.values, mode="lines",
                   name="union avg delay (ms)", line=dict(dash="dash")),
        secondary_y=True
    )

fig.update_layout(
    title=title,
    hovermode="x unified",
    margin=dict(l=60, r=20, t=60, b=40),
)
fig.update_xaxes(title="Time (UTC)")
fig.update_yaxes(title=f"p{int(PCTL*100)} gap (seconds)", type="log",
                 exponentformat="power", minor=dict(showgrid=True), secondary_y=False)
fig.update_yaxes(title="Average delay (ms)", type="linear", secondary_y=True)

fig.show()
fig.write_html(HTML_OUT, include_plotlyjs="cdn", full_html=True)
print(f"Wrote {HTML_OUT}.")
