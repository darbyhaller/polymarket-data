import pandas as pd

# Replace with your parquet filename
filename = "parquets/event_type=price_change/year=2025/month=09/day=13/hour=21/events-001.parquet"

df = pd.read_parquet(filename)
print(df.dtypes)
print([len(c) for c in df['changes']])

print(df['market'])