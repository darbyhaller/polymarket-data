import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

filename = "parquets/event_type=order_update_BUY/year=2025/month=09/day=14/hour=19/events-003.parquet"

# Load
df = pd.read_parquet(filename)

# Sort by event-time and (optionally) discard the first N rows (initial dump)
N_SKIP = 100_000
df = df.sort_values("timestamp", kind="mergesort")
if len(df) > N_SKIP:
    df = df.iloc[N_SKIP:, :]

# Compute delta in seconds (both are ms)
df["delta_sec"] = (df["recv_ts_ms"] - df["timestamp"]) / 1000.0

df["event_time"] = pd.to_datetime(df["recv_ts_ms"], unit="ms", utc=True)

# Plot
plt.figure(figsize=(12, 4))
plt.plot(df["event_time"], df["delta_sec"], linestyle="", marker=".", markersize=1)
plt.xlabel("Event time (UTC)")
plt.ylabel("recv_ts_ms - timestamp (seconds)")
plt.title("Network/processing lag over time")
plt.tight_layout()
plt.show()
