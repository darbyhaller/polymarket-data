import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import glob
import re

# Treat any jump > this many seconds as a gap
GAP_SEC = 1.0

# Find only the 001 parquets
files = sorted(glob.glob(
    "parquets/event_type=order_update_SELL/year=2025/month=09/day=*/hour=*/events-001.parquet"
))

row_re = re.compile(r".*year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)/events-001\.parquet$")

records = []
for f in files:
    m = row_re.match(f)
    if not m:
        continue
    year, month, day, hour = map(int, m.groups())
    if day > 16:
        continue

    # Hour window (UTC) derived from the path
    start = pd.Timestamp(year=year, month=month, day=day, hour=hour, tz="UTC")
    end   = start + pd.Timedelta(hours=1)
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(end.timestamp() * 1000)

    # Load only what's needed
    df = pd.read_parquet(f, columns=["timestamp"])
    # Clean + filter to the hour boundaries from the path
    ts = pd.to_numeric(df["timestamp"], errors="coerce").dropna().astype("int64").to_numpy()
    ts = ts[(ts >= start_ms) & (ts < end_ms)]
    ts.sort()

    # Count gaps within that hour
    if ts.size >= 2:
        gaps = np.diff(ts / 1000.0)  # seconds
        gap_count = int((gaps > GAP_SEC).sum())
    else:
        gap_count = 0

    records.append({
        "file": f,
        "file_hour_start": start,   # x-axis
        "gap_count": gap_count,
        "events_in_hour": int(ts.size)
    })

gaps_summary = pd.DataFrame(records).sort_values("file_hour_start")

# ---- Plot per-file (hourly) gap counts ----
plt.figure(figsize=(12, 5))
plt.plot(gaps_summary["file_hour_start"], gaps_summary["gap_count"], marker="o")
plt.xlabel("File hour (UTC)")
plt.ylabel(f"Timestamp gaps (> {GAP_SEC:.0f}s) per file")
plt.title("Gaps per events-001 file over time (hour-filtered)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Optional: quick daily aggregation if you want a higher-level view
daily = (gaps_summary
         .assign(day=gaps_summary["file_hour_start"].dt.floor("D"))
         .groupby("day", as_index=False)["gap_count"].sum())

print("Hourly summary (first 10 rows):")
print(gaps_summary.head(10)[["file_hour_start", "gap_count", "events_in_hour"]])

print("\nDaily totals:")
print(daily)
