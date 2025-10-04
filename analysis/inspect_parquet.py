#!/usr/bin/env python3
import pandas as pd
import glob

# ---------- Config ----------
GLOB_PATH     = "./us-east1-parquets/parquets/price_change_BUY/year=2025/month=10/day=01/hour=2*/events-*.parquet"
BASELINE_PATH = "./us-central1-parquets/parquets/price_change_BUY/year=2025/month=10/day=01/hour=2*/events-*.parquet"

# Add near the top:
KEYS = ["timestamp", "asset_hash", "size", "price", "best_bid", "best_ask"]

def load_parquet_all(pattern):
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"No shard files found: {pattern}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

def load_parquet_middle(pattern):
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"No shard files found: {pattern}")
    n = len(files)
    if n >= 4:
        q = n // 4
        files = files[q:n - q]  # middle half
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

# Replace your existing loads with:
print("[load] Reading SHARD (GLOB) - ALL files…")
df_glob_all = load_parquet_all(GLOB_PATH)
print("[load] Reading BASELINE - ALL files…")
df_base_all = load_parquet_all(BASELINE_PATH)
print("[load] Reading SHARD (GLOB) - MIDDLE HALF…")
df_glob_mid = load_parquet_middle(GLOB_PATH)
print("[load] Reading BASELINE - MIDDLE HALF…")
df_base_mid = load_parquet_middle(BASELINE_PATH)

# Build unique-tuple sets
def uniq_tuples(df): 
    return set(map(tuple, df[KEYS].drop_duplicates().itertuples(index=False, name=None)))

u_glob_all = uniq_tuples(df_glob_all)
u_base_all = uniq_tuples(df_base_all)
u_glob_mid = uniq_tuples(df_glob_mid)
u_base_mid = uniq_tuples(df_base_mid)

# Containment checks:
# 1) Is BASELINE middle-half contained in GLOB all?
miss_base_in_glob = u_base_mid - u_glob_all
pct_base_mid_not_in_glob = (len(miss_base_in_glob) / max(1, len(u_base_mid))) * 100.0

# 2) Is GLOB middle-half contained in BASELINE all?
miss_glob_in_base = u_glob_mid - u_base_all
pct_glob_mid_not_in_base = (len(miss_glob_in_base) / max(1, len(u_glob_mid))) * 100.0

print("\n=== Containment on unique combos (timestamp, asset_hash, size, price, best_bid, best_ask) ===")
print(f"Baseline MID unique combos: {len(u_base_mid)}")
print(f"GLOB ALL unique combos:     {len(u_glob_all)}")
print(f"Missing (Baseline MID not in GLOB ALL): {len(miss_base_in_glob)} "
      f"({pct_base_mid_not_in_glob:.4f}%)")

print(f"\nGLOB MID unique combos:     {len(u_glob_mid)}")
print(f"Baseline ALL unique combos: {len(u_base_all)}")
print(f"Missing (GLOB MID not in Baseline ALL): {len(miss_glob_in_base)} "
      f"({pct_glob_mid_not_in_base:.4f}%)")

# Optional: write samples to files for inspection
with open("missing_baseline_mid_in_glob_all.txt", "w") as f:
    for t in list(miss_base_in_glob)[:10000]:
        f.write(repr(t) + "\n")
with open("missing_glob_mid_in_baseline_all.txt", "w") as f:
    for t in list(miss_glob_in_base)[:10000]:
        f.write(repr(t) + "\n")

print("\n✅ Wrote mismatch samples (if any) to:")
print("   missing_baseline_mid_in_glob_all.txt")
print("   missing_glob_mid_in_baseline_all.txt")
