#!/usr/bin/env python3
import os, re, glob
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# ========= Config (tweak these) =========
ROOT        = "parquets"            # root folder containing event_type=.../year=.../...
EVENT_TYPE  = "price_change_SELL"   # which event_type directory to read
DAY_FILTER  = "*"                   # e.g. "26" or "*" to include all days
HOUR_FILTER = "*"                   # e.g. "07" or "*" to include all hours

TIMESTAMP_COL = "timestamp"         # column containing event time in **milliseconds since epoch**
USE_RIGHT_EDGE = True               # assign gap to the later event's timestamp (right edge); False uses left edge

RESAMPLE   = "1T"                   # aggregation bucket size ("1T" = 1 minute)
PCTL       = 0.99999999                   # percentile for pXX (0.90 = p90)
HTML_OUT   = "per_minute_pctl_gap.html"
OPEN_BROWSER = True                 # if True, try to open in your default browser
pio.renderers.default = "browser" if OPEN_BROWSER else "notebook_connected"
# =======================================

# Match paths like .../year=YYYY/month=MM/day=DD/hour=HH/events-XYZ.parquet
row_re = re.compile(r".*year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)/events-\d+\.parquet$")

# Collect files
pattern = os.path.join(
    ROOT,
    f"{EVENT_TYPE}",
    "year=*",
    "month=*",
    f"day={DAY_FILTER}",
    f"hour={HOUR_FILTER}",
    "events-*.parquet",
)
files = sorted(glob.glob(pattern))
if not files:
    raise RuntimeError(f"No parquet files matched: {pattern}")

# Nominal start time from the FIRST file’s path (used to clamp)
m = row_re.match(files[0])
if not m:
    raise RuntimeError(f"Could not parse time from filename: {files[0]}")
y, mth, d, h = map(int, m.groups())
file_start = pd.Timestamp(y, mth, d, h, tz="UTC")

# Load timestamps (expect ms since epoch; keep only numeric)
all_ts = []
for f in files:
    try:
        df = pd.read_parquet(f, columns=[TIMESTAMP_COL])
        ts = pd.to_numeric(df[TIMESTAMP_COL], errors="coerce").dropna().astype("int64").to_numpy()
        if ts.size:
            all_ts.append(ts)
    except Exception as e:
        print(f"Warning: failed to read {f}: {e}")

if not all_ts:
    raise RuntimeError("No timestamps found in matched files.")

# Flatten, sort, and clamp to nominal start
ts = np.sort(np.concatenate(all_ts))
ts = ts[ts >= int(file_start.value // 10**6)]
if ts.size < 2:
    raise RuntimeError("Fewer than 2 timestamps after clamping; cannot compute gaps.")

# Compute inter-event gaps in **seconds**
gaps_sec = np.diff(ts / 1000.0)

# Assign each gap to a timestamp (right or left edge), then to minute buckets
edge = ts[1:] if USE_RIGHT_EDGE else ts[:-1]
gap_time = pd.to_datetime(edge, unit="ms", utc=True)

# Build a Series and aggregate by minute with the desired percentile
s = pd.Series(gaps_sec, index=gap_time)
per_minute_p = s.resample(RESAMPLE).quantile(PCTL)

# Optional sanity: drop NaNs (minutes with no gaps)
per_minute_p = per_minute_p.dropna()
if per_minute_p.empty:
    raise RuntimeError("No per-minute data to plot (no gaps found in any minute).")

# ---------- Plotly figure (single, interactive) ----------
title = f"{int(PCTL*100)}th percentile of gaps per {RESAMPLE} (UTC) — event_type={EVENT_TYPE}"
fig = make_subplots(specs=[[{"secondary_y": False}]])
fig.add_trace(
    go.Scatter(
        x=per_minute_p.index,
        y=per_minute_p.values,
        mode="lines",
        name=f"p{int(PCTL*100)} gap (s)",
        hovertemplate="Minute: %{x|%Y-%m-%d %H:%M}<br>pXX gap: %{y:.6g}s<extra></extra>",
    )
)

fig.update_layout(
    title=title,
    hovermode="x unified",
    margin=dict(l=60, r=20, t=60, b=40),
)
fig.update_xaxes(title="Time (UTC)")
fig.update_yaxes(title=f"p{int(PCTL*100)} gap (seconds)", rangemode="tozero")

# Show and also write a lightweight HTML for sharing
fig.show()
fig.write_html(HTML_OUT, include_plotlyjs="cdn", full_html=True)
print(f"Wrote {HTML_OUT}.")
