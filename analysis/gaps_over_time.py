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
    "us-central1", "us-east1", "europe-west4", 
    "asia-northeast1", "asia-southeast1", "australia-southeast1",
    "me-west1", "southamerica-east1", "africa-south1",
]
ROOTS = {root: "all-parquets/" + root + "-parquets/parquets" for root in roots}

EVENT_TYPE   = "price_change_BUY"
DAY_FILTER   = "05"   # "*" for all
HOUR_FILTER  = "08"    # "*" for all

TIMESTAMP_COL  = "timestamp"         # ms since epoch
USE_RIGHT_EDGE = True
RESAMPLE       = "1s"     # 1T = 1 minute
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
    for f in files:
        df = pd.read_parquet(f, columns=[TIMESTAMP_COL])
        ts = pd.to_numeric(df[TIMESTAMP_COL], errors="coerce").dropna().astype("int64").to_numpy()
        if ts.size:
            all_ts.append(ts)

    if not all_ts:
        raise RuntimeError(f"No timestamps found in matched files for {root}")

    ts = np.sort(np.concatenate(all_ts))
    ts = ts[ts >= int(file_start.value // 10**6)]
    if ts.size < 2:
        raise RuntimeError(f"Fewer than 2 timestamps after clamping for {root}")

    # percentile gaps per RESAMPLE (seconds)
    gaps_sec = np.diff(ts / 1000.0)
    edge = ts[1:] if USE_RIGHT_EDGE else ts[:-1]
    gap_time = pd.to_datetime(edge, unit="ms", utc=True)
    s_gap = pd.Series(gaps_sec, index=gap_time).resample(RESAMPLE).quantile(PCTL).dropna()

    return s_gap, ts

# ---- per-region series + raw timestamps ----
series_by_region = {}
ts_by_region = {}

for label, path in ROOTS.items():
    s_gap, ts = load_series_for_root(path)
    series_by_region[label] = s_gap
    ts_by_region[label] = ts

# ---- union across all regions ----
all_ts = np.sort(np.concatenate(list(ts_by_region.values())))
gaps_union = np.diff(all_ts / 1000.0)
edge_union = all_ts[1:] if USE_RIGHT_EDGE else all_ts[:-1]
t_union = pd.to_datetime(edge_union, unit="ms", utc=True)
s_union = pd.Series(gaps_union, index=t_union).resample(RESAMPLE).quantile(PCTL).dropna()

# ---- plotting ----
palette = (pq.Set1 + pq.Set2 + pq.Set3 + pq.Dark24 + pq.Light24)
title = f"p{int(PCTL*100)} gaps per {RESAMPLE} — {EVENT_TYPE}"

fig = make_subplots(specs=[[{"secondary_y": False}]])

# Gaps (log scale)
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

fig.update_layout(
    title=title,
    hovermode="x unified",
    margin=dict(l=60, r=20, t=60, b=40),
)
fig.update_xaxes(title="Time (UTC)")
fig.update_yaxes(title=f"p{int(PCTL*100)} gap (seconds)", type="log",
                 exponentformat="power", minor=dict(showgrid=True))

fig.show()
fig.write_html(HTML_OUT, include_plotlyjs="cdn", full_html=True)
print(f"Wrote {HTML_OUT}.")
