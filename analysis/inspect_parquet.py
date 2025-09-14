import pandas as pd

# Replace with your parquet filename
filename = "parquets/event_type=order_update_BUY/year=2025/month=09/day=14/hour=18/events-000.parquet"

df = pd.read_parquet(filename)
print(df.dtypes)

print(df.head())