import pyarrow.parquet as pq
print(pq.read_table("./events-003.parquet").schema)