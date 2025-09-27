#!/usr/bin/env python3
import pandas as pd
import glob

# ---------- Config ----------
GLOB_PATH     = "./tp/parquets/price_change_BUY/year=2025/month=09/day=27/hour=22/events-001.parquet"
BASELINE_PATH = "./events-001.parquet"
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

# ---------- Asset intersections ----------
shard_assets = set(df_shard["asset_hash"].unique())
base_assets  = set(df_base["asset_hash"].unique())

intersection = shard_assets & base_assets

print("\n=== Asset ID Intersection ===")
print(f"Shard total unique:    {len(shard_assets)}")
print(f"Baseline total unique: {len(base_assets)}")
print(f"Intersection count:    {len(intersection)}")

# Show a sample of overlapping IDs
for asset in list(intersection)[:50]:
    print(asset)

# Optional: write to file
with open("asset_intersection.txt", "w") as f:
    for asset in sorted(intersection):
        f.write(str(asset) + "\n")

print("\n✅ Wrote intersection list to asset_intersection.txt")
