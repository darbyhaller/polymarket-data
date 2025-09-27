#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import glob

# ---------- Config ----------
GLOB_PATH     = "./tp/parquets/event_type=order_update_BUY/year=2025/month=09/day=27/hour=18/events-00*.parquet"
BASELINE_PATH = "./events-003.parquet"
# -----------------------------

def load_parquet(pattern):
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"No shard files found: {pattern}")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

print("[load] Reading shard parquet(s)…")
df_shard = load_parquet(GLOB_PATH)
print("[load] Reading baseline parquet…")
df_base = pd.read_parquet(BASELINE_PATH)

# ---------- Find intersection assets ----------
shard_counts = df_shard["asset_hash"].value_counts()
base_counts  = df_base["asset_hash"].value_counts()

intersection = set(shard_counts.index) & set(base_counts.index)
if not intersection:
    raise SystemExit("❌ No overlapping assets between shard and baseline!")

# Compute min(row count) for each common asset
stats = []
for asset in intersection:
    s_count = shard_counts.get(asset, 0)
    b_count = base_counts.get(asset, 0)
    stats.append((asset, s_count, b_count, min(s_count, b_count)))

stats_df = pd.DataFrame(stats, columns=["asset_hash", "shard_rows", "baseline_rows", "min_rows"])
best_asset = stats_df.sort_values("min_rows", ascending=False).iloc[0]["asset_hash"]

print("\n=== Asset Selection ===")
print(stats_df.sort_values("min_rows", ascending=False).head(10))
print(f"\n[info] Selected asset: {best_asset}")

# ---------- Filter ----------
df_shard_a = df_shard[df_shard["asset_hash"] == best_asset].copy()
df_base_a  = df_base[df_base["asset_hash"] == best_asset].copy()

print(f"[info] Shard rows for asset: {len(df_shard_a)}")
print(f"[info] Baseline rows for asset: {len(df_base_a)}")

# ---------- Time normalization ----------
df_shard_a["timestamp"] = pd.to_datetime(df_shard_a["timestamp"], unit="ms", utc=True)
df_base_a["timestamp"]  = pd.to_datetime(df_base_a["timestamp"], unit="ms", utc=True)

# ---------- Sort ----------
df_shard_a = df_shard_a.sort_values("timestamp")
df_base_a  = df_base_a.sort_values("timestamp")

# ---------- Plot ----------
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# Price series
ax1.plot(df_shard_a["timestamp"], df_shard_a["price"], label="Shard", color="C0", alpha=0.7)
ax1.plot(df_base_a["timestamp"], df_base_a["price"], label="Baseline", color="C1", alpha=0.7, linestyle="--")
ax1.set_ylabel("Price")
ax1.legend()
ax1.grid(True, ls="--", alpha=0.5)

# Size series
ax2.plot(df_shard_a["timestamp"], df_shard_a["size"], label="Shard", color="C0", alpha=0.7)
ax2.plot(df_base_a["timestamp"], df_base_a["size"], label="Baseline", color="C1", alpha=0.7, linestyle="--")
ax2.set_ylabel("Size")
ax2.set_xlabel("Timestamp")
ax2.legend()
ax2.grid(True, ls="--", alpha=0.5)

fig.suptitle(f"Per-Asset Event Comparison — asset_hash = {best_asset}")
fig.tight_layout()
plt.show()
