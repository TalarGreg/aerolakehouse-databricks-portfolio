# Databricks notebook source
# MAGIC %md
# MAGIC # Validate OpenSky Silver
# MAGIC
# MAGIC Validate the records processed by the current triggered pipeline update.

# COMMAND ----------

from datetime import datetime
import re

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "dev_alh", "Unity Catalog catalog")
dbutils.widgets.text("run_ts", "", "Parent job start timestamp")

catalog = dbutils.widgets.get("catalog").strip()
run_ts_raw = dbutils.widgets.get("run_ts").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Invalid catalog identifier: {catalog!r}")
if not run_ts_raw:
    raise ValueError("run_ts is required for update-scoped validation")

run_ts = datetime.fromisoformat(run_ts_raw.replace("Z", "+00:00"))
main_table = f"{catalog}.silver.opensky_state"
quarantine_table = f"{catalog}.silver.opensky_state_quarantine"

# COMMAND ----------

missing_tables = [
    table_name
    for table_name in (main_table, quarantine_table)
    if not spark.catalog.tableExists(table_name)
]
if missing_tables:
    raise AssertionError(f"Missing OpenSky Silver tables: {missing_tables}")

current_main = spark.table(main_table).where(
    F.col("silver_processed_ts") >= F.lit(run_ts)
)
current_quarantine = spark.table(quarantine_table).where(
    F.col("silver_processed_ts") >= F.lit(run_ts)
)

main_count = current_main.count()
quarantine_count = current_quarantine.count()

# COMMAND ----------

invalid_main_count = current_main.where(
    F.col("icao24").isNull()
    | ~F.col("icao24").rlike(r"^[0-9A-F]{6}$")
    | F.col("snapshot_ts").isNull()
).count()

invalid_quarantine_reason_count = current_quarantine.where(
    F.col("quarantine_reasons").isNull()
    | (F.size("quarantine_reasons") == 0)
).count()

if invalid_main_count:
    raise AssertionError(
        f"OpenSky Silver contains {invalid_main_count} record(s) with invalid critical keys"
    )
if invalid_quarantine_reason_count:
    raise AssertionError(
        "OpenSky quarantine contains records without a quarantine reason"
    )

if main_count + quarantine_count == 0:
    print("WARNING: the current pipeline update produced no new OpenSky Silver records")

print(
    "OpenSky Silver validation succeeded: "
    f"main_rows={main_count}, quarantine_rows={quarantine_count}, run_ts={run_ts.isoformat()}"
)
