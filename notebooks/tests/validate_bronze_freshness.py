# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Freshness & Load Validation
# MAGIC Phase 3 of the `bronze_ingestion_e2e` job. Confirms that the Bronze load produced
# MAGIC fresh, correct data for the current pipeline run.
# MAGIC
# MAGIC **Inputs**
# MAGIC - `pipeline_run_id` — read from the upstream `run_bronze_ingestion` task via task values
# MAGIC   (falls back to the most recent run in the audit table for interactive runs).
# MAGIC - `run_ts` — the job start time (`{{job.start_time.iso_datetime}}`), used as the
# MAGIC   freshness reference (falls back to the run's earliest `start_ts` in the audit table).
# MAGIC
# MAGIC **Assertion contract** (per source in the run's audit rows):
# MAGIC - `SUCCESS` and `rows_written > 0` → assert `MAX(_bronze_ingest_ts) >= run_ts`.
# MAGIC - `SUCCESS` and `rows_written == 0` → skipped from freshness (empty pull / SKIPPED),
# MAGIC   reported as a warning so a silently-failed upstream ingest stays visible in the logs.
# MAGIC - `FAILED` → collected and reported; fails the validation.
# MAGIC - OurAirports (`our_airports_*`) → SMOKE CHECK: even when `rows_written == 0`
# MAGIC   (copy_into SKIPPED because the file was unchanged), assert the table exists AND
# MAGIC   `COUNT(*) > 0`.
# MAGIC
# MAGIC The notebook raises at the end if any assertion failed, turning the job task red.

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalog",     "dev_alh")
dbutils.widgets.text("audit_table", "{catalog}.monitoring.ingestion_audit")
dbutils.widgets.text("run_ts",      "")

catalog     = dbutils.widgets.get("catalog")
audit_table = dbutils.widgets.get("audit_table").replace("{catalog}", catalog)
run_ts_raw  = dbutils.widgets.get("run_ts")

# COMMAND ----------

# Resolve the pipeline run id from the upstream run_bronze_ingestion task.
# Falls back to the latest audit run when executed interactively (no task values).
pipeline_run_id = ""
try:
    pipeline_run_id = dbutils.jobs.taskValues.get(
        taskKey="run_bronze_ingestion",
        key="pipeline_run_id",
        debugValue="",
    )
except Exception as exc:
    print(f"taskValues.get skipped ({exc}); will fall back to latest audit run.")

if not pipeline_run_id:
    latest = spark.sql(
        f"SELECT pipeline_run_id FROM {audit_table} ORDER BY start_ts DESC LIMIT 1"
    ).collect()
    if not latest:
        raise RuntimeError(f"No audit records found in {audit_table}; nothing to validate.")
    pipeline_run_id = latest[0]["pipeline_run_id"]
    print(f"No task value found — falling back to latest run: {pipeline_run_id}")

print(f"Validating pipeline_run_id : {pipeline_run_id}")

# COMMAND ----------

def _normalize_ts(value: str) -> str:
    """Normalize an ISO-8601 string to 'yyyy-MM-dd HH:mm:ss' for Spark to_timestamp()."""
    if not value:
        return ""
    v = value.strip().replace("T", " ")
    if v.endswith("Z"):
        v = v[:-1]
    # Drop timezone offset / fractional seconds if present.
    return v[:19].strip()

run_ts = _normalize_ts(run_ts_raw)

# When the job template is not rendered (interactive run) fall back to the run's start.
if not run_ts or "{{" in run_ts_raw:
    row = spark.sql(
        f"SELECT MIN(start_ts) AS min_start FROM {audit_table} "
        f"WHERE pipeline_run_id = '{pipeline_run_id}'"
    ).collect()[0]
    if row["min_start"] is None:
        raise RuntimeError(
            f"No audit rows for run {pipeline_run_id}; cannot derive freshness reference."
        )
    run_ts = row["min_start"].strftime("%Y-%m-%d %H:%M:%S")
    print(f"run_ts not supplied — using run start from audit: {run_ts}")

print(f"Freshness reference (run_ts): {run_ts}")

# COMMAND ----------

audit_rows = spark.sql(f"""
    SELECT source_name, target_table, status, COALESCE(rows_written, 0) AS rows_written
    FROM {audit_table}
    WHERE pipeline_run_id = '{pipeline_run_id}'
""").collect()

if not audit_rows:
    raise RuntimeError(
        f"No audit rows found for pipeline_run_id={pipeline_run_id} in {audit_table}."
    )

print(f"Audit rows for this run: {len(audit_rows)}")

# COMMAND ----------

def _table_probe(table: str):
    """Return (exists, count, max_ingest_ts, is_fresh) for a Bronze target table."""
    try:
        r = spark.sql(f"""
            SELECT COUNT(*)                    AS cnt,
                   MAX(_bronze_ingest_ts)      AS max_ts,
                   (MAX(_bronze_ingest_ts) >= to_timestamp('{run_ts}')) AS is_fresh
            FROM {table}
        """).collect()[0]
        return True, r["cnt"], r["max_ts"], bool(r["is_fresh"])
    except Exception as exc:
        print(f"  ! could not probe {table}: {exc}")
        return False, 0, None, False

# Buckets for the final report.
fresh_ok, warnings, stale, smoke_failed, load_failed = [], [], [], [], []

for row in audit_rows:
    source = row["source_name"]
    table  = row["target_table"]
    status = row["status"]
    rows   = row["rows_written"]
    is_ref = source.startswith("our_airports")

    if status == "FAILED":
        load_failed.append(f"{source} (audit status FAILED)")
        print(f"[{source}] FAILED in audit — load error.")
        continue

    exists, cnt, max_ts, is_fresh = _table_probe(table)

    if is_ref:
        # Reference data: smoke check existence + non-empty regardless of rows_written,
        # because copy_into reports SKIPPED (rows_written == 0) when the file is unchanged.
        if not exists or cnt == 0:
            smoke_failed.append(f"{source} -> {table} (exists={exists}, count={cnt})")
            print(f"[{source}] SMOKE FAILED — table empty or missing.")
            continue
        if rows > 0 and not is_fresh:
            stale.append(f"{source} -> {table} (max_ts={max_ts} < run_ts={run_ts})")
            print(f"[{source}] STALE — wrote {rows} rows but max_ts {max_ts} < {run_ts}.")
        else:
            fresh_ok.append(source)
            reason = "fresh write" if rows > 0 else f"SKIPPED (unchanged), {cnt} existing rows"
            print(f"[{source}] OK — {reason}.")
        continue

    # Event / API sources.
    if rows > 0:
        if is_fresh:
            fresh_ok.append(source)
            print(f"[{source}] OK — {rows} rows, max_ts {max_ts} >= {run_ts}.")
        else:
            stale.append(f"{source} -> {table} (max_ts={max_ts} < run_ts={run_ts})")
            print(f"[{source}] STALE — wrote {rows} rows but max_ts {max_ts} < {run_ts}.")
    else:
        # rows_written == 0: an empty pull is acceptable, but flag it so a silently
        # failed upstream ingest remains visible in the logs.
        warnings.append(f"{source} -> {table} (rows_written=0; possible empty pull or upstream ingest issue)")
        print(f"[{source}] WARNING — 0 rows written; skipped from freshness.")

# COMMAND ----------

print("\n" + "=" * 60)
print(f"Bronze validation — pipeline_run_id {pipeline_run_id}")
print(f"Freshness reference : {run_ts}")
print(f"Fresh / OK          : {len(fresh_ok)}  {fresh_ok}")
print(f"Warnings (0 rows)   : {len(warnings)}")
for w in warnings:
    print(f"    - {w}")
print(f"Stale               : {len(stale)}")
for s in stale:
    print(f"    - {s}")
print(f"Smoke failed        : {len(smoke_failed)}")
for s in smoke_failed:
    print(f"    - {s}")
print(f"Load failed         : {len(load_failed)}")
for s in load_failed:
    print(f"    - {s}")
print("=" * 60)

problems = load_failed + stale + smoke_failed
if problems:
    raise RuntimeError(
        f"Bronze validation FAILED for {len(problems)} source(s):\n"
        + "\n".join(f"  - {p}" for p in problems)
    )

print("Bronze validation PASSED.")
