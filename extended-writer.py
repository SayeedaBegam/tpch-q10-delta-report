# extended-writer.py  (streaming + light clustering for better skipping)
import duckdb
from deltalake import write_deltalake

SCALE_FACTOR = 10  # increase to 100+ for bigger datasets

con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch;")
con.execute(f"CALL dbgen(sf={SCALE_FACTOR});")

# Stream a DuckDB query as Arrow RecordBatchReader (no big pandas DataFrames)
def stream(query, rows_per_batch=100_000):
    return con.execute(query).fetch_record_batch(rows_per_batch=rows_per_batch)

print("Writing Delta tables (streaming, no pandas)..")

# ---- Baseline (non-partitioned) ----
write_deltalake("./orders-delta",   stream("SELECT * FROM orders"),   mode="overwrite")
write_deltalake("./lineitem-delta", stream("SELECT * FROM lineitem"), mode="overwrite")
write_deltalake("./customer-delta", stream("SELECT * FROM customer"), mode="overwrite")
write_deltalake("./nation-delta",   stream("SELECT * FROM nation"),   mode="overwrite")

# ---- Partitioned versions for Q10 file skipping ----
# Add year/month and lightly cluster to tighten per-file min/max stats.

orders_part_sql = """
SELECT
  *,
  CAST(EXTRACT(year  FROM o_orderdate) AS INT) AS o_orderdate_year,
  CAST(EXTRACT(month FROM o_orderdate) AS INT) AS o_orderdate_month
FROM orders
ORDER BY o_orderdate_year, o_orderdate_month, o_orderkey
"""

write_deltalake(
    "./orders-delta-part",
    stream(orders_part_sql),
    mode="overwrite",
    partition_by=["o_orderdate_year", "o_orderdate_month"],
)

# Cluster lineitem by (l_returnflag, l_orderkey) before partitioned write
write_deltalake(
    "./lineitem-delta-part",
    stream("SELECT * FROM lineitem ORDER BY l_returnflag, l_orderkey"),
    mode="overwrite",
    partition_by=["l_returnflag"],
)

print("✅ Wrote baseline and partitioned Delta tables (streaming + clustered).")
