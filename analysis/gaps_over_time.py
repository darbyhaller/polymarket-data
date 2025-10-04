#!/usr/bin/env python3
import os, re, glob
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# ========= Config (tweak these) =========
ROOTS      = ["us-central1-parquets/parquets", "us-east1-parquets/parquets"]
EVENT_TYPE = "price_change_BUY"
DAY_FILTER = "*"
HOUR_FILTER = "*"

TIMESTAMP_COL = "timestamp"         # ms since epoch
USE_RIGHT_EDGE = True
RESAMPLE = "1T"
PCTL = 0.9999999
HTML_OUT = "per_minute_pctl_gap_multi.html"
OPEN_BROWSER = True
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

    gaps_sec = np.diff(ts / 1000.0)
    edge = ts[1:] if USE_RIGHT_EDGE else ts[:-1]
    gap_time = pd.to_datetime(edge, unit="ms", utc=True)
    s = pd.Series(gaps_sec, index=gap_time).resample(RESAMPLE).quantile(PCTL)
    return s.dropna(), ts

# Load per-root series + raw timestamps
s_central, ts_central = load_series_for_root(ROOTS[0])
s_east,    ts_east    = load_series_for_root(ROOTS[1])

# Union: combine raw timestamps then repeat the gap→minute→percentile pipeline
ts_union = np.sort(np.concatenate([ts_central, ts_east]))
gaps_union = np.diff(ts_union / 1000.0)
edge_union = ts_union[1:] if USE_RIGHT_EDGE else ts_union[:-1]
t_union = pd.to_datetime(edge_union, unit="ms", utc=True)
s_union = pd.Series(gaps_union, index=t_union).resample(RESAMPLE).quantile(PCTL).dropna()

# (Optional) log scale — uncomment if you want log Y
# s_central = s_central[s_central > 0]
# s_east = s_east[s_east > 0]
# s_union = s_union[s_union > 0]

title = f"p{int(PCTL*100)} gaps per {RESAMPLE} — {EVENT_TYPE} (red: us-central1, green: us-east1, blue: union)"

fig = make_subplots(specs=[[{"secondary_y": False}]])
fig.add_trace(go.Scatter(x=s_central.index, y=s_central.values, mode="lines",
                         name="us-central1", line=dict(color="red")))
fig.add_trace(go.Scatter(x=s_east.index, y=s_east.values, mode="lines",
                         name="us-east1", line=dict(color="green")))
fig.add_trace(go.Scatter(x=s_union.index, y=s_union.values, mode="lines",
                         name="union", line=dict(color="blue")))

fig.update_layout(
    title=title,
    hovermode="x unified",
    margin=dict(l=60, r=20, t=60, b=40),
)
fig.update_xaxes(title="Time (UTC)")
fig.update_yaxes(title=f"p{int(PCTL*100)} gap (seconds)")
fig.update_yaxes(type="log", exponentformat="power", minor=dict(showgrid=True))

fig.show()
fig.write_html(HTML_OUT, include_plotlyjs="cdn", full_html=True)
print(f"Wrote {HTML_OUT}.")
