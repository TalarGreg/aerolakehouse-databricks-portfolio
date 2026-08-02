# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Runner
# MAGIC Loads all configured sources from Volume raw_files → Bronze Delta tables.
# MAGIC
# MAGIC - **append** sources: Auto Loader with `availableNow=True` — processes only new files since last checkpoint, then exits
# MAGIC - **copy_into** sources: append only reference files not yet recorded by Delta
# MAGIC
# MAGIC **Before first run:**
# MAGIC 1. Execute `sql/ddl/010_create_monitoring_ingestion_audit.sql` in SQL Editor
# MAGIC 2. Execute `sql/ddl/030_create_our_airports_bronze_tables.sql` in SQL Editor
# MAGIC 3. Run pipeline once, then execute `sql/ddl/040_set_bronze_tblproperties.sql`

# COMMAND ----------

import sys
import uuid

# Resolve the CI/CD-deployed bundle root from this notebook's Workspace path.
# DAB deploys to: /Users/<email>/.bundle/aerolakehouse/dev/files/
# Notebook is at: .../files/notebooks/bronze/run_bronze_ingestion
# files/ root is 3 levels up from the notebook path.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_files_root    = "/Workspace" + "/".join(_notebook_path.split("/")[:-3])
_src_path      = f"{_files_root}/src"

sys.path.insert(0, _src_path)

print(f"Notebook path   : {_notebook_path}")
print(f"Files root      : {_files_root}")
print(f"sys.path[0]     : {_src_path}")

# COMMAND ----------

dbutils.widgets.text("catalog",     "dev_alh")
dbutils.widgets.text("config_path", f"{_files_root}/configs/bronze_sources.yml")
dbutils.widgets.text("audit_table", "{catalog}.monitoring.ingestion_audit")

catalog     = dbutils.widgets.get("catalog")
config_path = dbutils.widgets.get("config_path")
audit_table = dbutils.widgets.get("audit_table").replace("{catalog}", catalog)

pipeline_run_id = str(uuid.uuid4())

print(f"Pipeline run ID : {pipeline_run_id}")
print(f"Catalog         : {catalog}")
print(f"Config path     : {config_path}")
print(f"Audit table     : {audit_table}")

# Expose the run id to downstream job tasks (e.g. validate_bronze_freshness).
# No-op when the notebook is run interactively outside a Databricks Job.
try:
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=pipeline_run_id)
    print("pipeline_run_id published to task values.")
except Exception as _tv_exc:
    print(f"taskValues.set skipped (not in a job context): {_tv_exc}")

# COMMAND ----------

import importlib
import bronze_ingestion.opensky_schema
import bronze_ingestion.flatten
import bronze_ingestion.config
import bronze_ingestion.security
import bronze_ingestion.bronze_loader
importlib.reload(bronze_ingestion.opensky_schema)
importlib.reload(bronze_ingestion.flatten)
importlib.reload(bronze_ingestion.config)
importlib.reload(bronze_ingestion.security)
importlib.reload(bronze_ingestion.bronze_loader)

from bronze_ingestion.config import load_config
from bronze_ingestion.bronze_loader import ingest_source_to_bronze
from bronze_ingestion.security import sanitize_exception

sources = load_config(config_path, catalog=catalog)

print(f"Loaded {len(sources)} source(s):")
for s in sources:
    print(f"  [{s['load_mode']:>9}] {s['source_name']} → {s['target_table']}")

# COMMAND ----------

failed_sources = []

for source_cfg in sources:
    source_name = source_cfg["source_name"]
    try:
        ingest_source_to_bronze(
            spark=spark,
            source_cfg=source_cfg,
            pipeline_run_id=pipeline_run_id,
            audit_table=audit_table,
            dbutils=dbutils,
        )
    except Exception as exc:
        # Error already written to audit table inside ingest_source_to_bronze
        failed_sources.append(
            {"source": source_name, "error": sanitize_exception(exc)}
        )
        print(f"[{source_name}] FAILED — continuing with remaining sources")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Pipeline run ID : {pipeline_run_id}")
print(f"Total sources   : {len(sources)}")
print(f"Succeeded       : {len(sources) - len(failed_sources)}")
print(f"Failed          : {len(failed_sources)}")

if failed_sources:
    summary = "\n".join(f"  - {f['source']}: {f['error']}" for f in failed_sources)
    print(f"\nFailed sources:\n{summary}")
    print("=" * 60)
    raise RuntimeError(
        f"Bronze ingestion FAILED for {len(failed_sources)} source(s):\n{summary}"
    )

print("All sources ingested successfully.")
print("=" * 60)
