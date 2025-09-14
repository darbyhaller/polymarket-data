import numpy as np
import pandas as pd

# Replace with your parquet filename
filename = "parquets/event_type=order_update_BUY/year=2025/month=09/day=14/hour=19/events-001.parquet"

df = pd.read_parquet(filename)
# print(df.dtypes)

ts = np.array(sorted(df['timestamp']))/1000
# ignore initial book dumps
ts = ts[100_000:-10_000]
gaps = np.diff(ts)
idxs = np.where(gaps > 1)[0]
print(len(idxs)/len(ts))
print(idxs/len(ts))
print(len(ts))

# print(df)