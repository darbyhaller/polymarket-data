import os, re, glob
import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.io as pio

# ========= Config =========
GAP_SEC         = 0.5
EVENT_TYPE      = "order_update_SELL"
ROOT            = "parquets"

# Interactivity & fidelity
MAX_POINTS_RCDF = 9000      # total point budget for the RCDF
HEAD_KEEP_FRAC  = 0.03      # keep exact first 3% of points (small gaps/head)
HEAD_KEEP_MIN   = 2500      # ...at least this many exact head points
TAIL_KEEP_FRAC  = 0.03      # keep exact last 3% of points (long tail)
TAIL_KEEP_MIN   = 2500      # ...at least this many exact tail points
PDF_BINS        = 140       # log-spaced bins for the PDF

HTML_OUT        = "gaps_rcdf_interactive.html"
pio.renderers.default = "browser"
# ==========================

# Match paths like .../year=YYYY/month=MM/day=DD/hour=HH/events-XXX.parquet
row_re = re.compile(r".*year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)/events-\d+\.parquet$")

# Find all shards
pattern = os.path.join(
    ROOT,
    f"event_type={EVENT_TYPE}",
    "year=*",
    "month=*",
    "day=26",
    "hour=*",
    "events-*.parquet",
)
files = sorted(glob.glob(pattern))
if not files:
    raise RuntimeError(f"No parquet files matched: {pattern}")

# Nominal start time from first file path
m = row_re.match(files[0])
if not m:
    raise RuntimeError(f"Could not parse time from filename: {files[0]}")
y, mth, d, h = map(int, m.groups())
file_start = pd.Timestamp(y, mth, d, h, tz="UTC")

# Load timestamps (expect ms since epoch in 'timestamp')
all_ts = []
for f in files:
    try:
        df = pd.read_parquet(f, columns=["timestamp"])
        ts = pd.to_numeric(df["timestamp"], errors="coerce").dropna().astype("int64").to_numpy()
        if ts.size:
            all_ts.append(ts)
    except Exception as e:
        print(f"Warning: failed to read {f}: {e}")

if not all_ts:
    raise RuntimeError("No timestamps found in matched files.")

# Flatten, sort, clamp to nominal start
ts = np.sort(np.concatenate(all_ts))
ts = ts[ts >= int(file_start.value // 10**6)]
if ts.size == 0:
    raise RuntimeError(f"All events were before nominal start {file_start}")

# Gaps (seconds)
gaps_sec = np.diff(ts / 1000.0)
if gaps_sec.size == 0:
    raise RuntimeError("Only one timestamp found; cannot compute gaps.")

# ------- Prepare RCDF (survival) with head+tail preservation -------
sorted_gaps = np.sort(gaps_sec)
n = sorted_gaps.size

# exact head and tail
head_keep = min(max(int(HEAD_KEEP_FRAC * n), HEAD_KEEP_MIN), n)
tail_keep = min(max(int(TAIL_KEEP_FRAC * n), TAIL_KEEP_MIN), n - head_keep)

head_end = head_keep
tail_start = max(n - tail_keep, head_end)

# Indices we will keep
keep_idx = []

# head exact
if head_end > 0:
    keep_idx.append(np.arange(0, head_end, dtype=int))

# middle thinned
mid_start = head_end
mid_end = tail_start
if mid_end > mid_start:
    # remaining budget for the middle
    remaining_budget = max(0, MAX_POINTS_RCDF - head_end - (n - tail_start))
    if remaining_budget > 0:
        mid_idx = np.linspace(mid_start, mid_end - 1, remaining_budget, dtype=int)
        keep_idx.append(mid_idx)
# tail exact
if tail_start < n:
    keep_idx.append(np.arange(tail_start, n, dtype=int))

keep_idx = np.unique(np.concatenate(keep_idx)) if keep_idx else np.arange(n)

x_vals = sorted_gaps[keep_idx]
# Empirical CDF at those x (≤ x)
F_x = (keep_idx + 1) / n
# Reverse CDF (strictly greater than x): S(x) = P(X > x) = 1 - F(x)
S_x = 1.0 - F_x

# ------- PDF (log-spaced bins) -------
pos = sorted_gaps[sorted_gaps > 0]
if pos.size == 0:
    raise RuntimeError("All gaps are zero; cannot plot on a log scale.")
gmin, gmax = pos.min(), pos.max()
if gmin == gmax:
    edges = np.array([gmin * 0.9, gmin, gmin * 1.1])
else:
    edges = np.logspace(np.log10(gmin), np.log10(gmax), PDF_BINS)

hist, edges = np.histogram(pos, bins=edges, density=True)
centers = np.sqrt(edges[:-1] * edges[1:])   # geometric centers for log-x bars
widths  = edges[1:] - edges[:-1]

# ------- Build single interactive figure -------
fig = make_subplots(specs=[[{"secondary_y": True}]])

# RCDF (WebGL) — head & tail exact, middle thinned
fig.add_trace(
    go.Scattergl(
        x=x_vals,
        y=S_x,
        mode="lines",
        name="RCDF: P(gap > x)",
        hovertemplate=(
            f"x (gap): %{{x:.6g}}s<br>"
            f"RCDF: %{{y:.6f}}<br>"
            f"count ≈ %{{customdata}} of {n}<extra></extra>"
        ),
        customdata=np.maximum((S_x * n).astype(int), 0),
    ),
    secondary_y=False,
)

# PDF bars (right y-axis)
fig.add_trace(
    go.Bar(
        x=centers,
        y=hist,
        width=widths,
        name="PDF",
        opacity=0.35,
        hovertemplate="Gap bin center: %{x:.6g}s<br>PDF: %{y:.6g}<extra></extra>",
    ),
    secondary_y=True,
)

fig.update_layout(
    title=f"Reverse CDF (survival) + PDF of event gaps (> {GAP_SEC}s considered a gap)",
    bargap=0,
    hovermode="x unified",
    xaxis=dict(
        type="log",
        title="Gap duration (seconds, log)",
        showspikes=True,
        spikemode="across",
        spikethickness=1,
    ),
    yaxis=dict(title="RCDF  (P[gaps > x])", rangemode="tozero"),
    yaxis2=dict(title="PDF", rangemode="tozero"),
    legend=dict(x=0.72, y=0.02),
)

fig.show()
fig.write_html(HTML_OUT, include_plotlyjs="cdn", full_html=True)
print(f"Wrote {HTML_OUT} — open it if the browser doesn’t auto-launch.")
