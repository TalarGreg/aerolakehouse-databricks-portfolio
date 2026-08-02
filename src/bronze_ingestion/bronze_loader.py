from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bronze_ingestion.audit import write_audit_record
from bronze_ingestion.flatten import apply_flatten
from bronze_ingestion.security import sanitize_exception


def _find_latest_file(dbutils: Any, path: str, extension: str = ".csv") -> tuple[str, int]:
    """Recursively find the newest file under path using dbutils.fs.ls().

    dbutils.fs.ls() is the recommended approach for UC Volumes — it uses the
    Unity Catalog FUSE layer directly from the driver, unlike binaryFile which
    uses Hadoop FileSystem glob APIs that are unreliable on /Volumes/ paths.

    Args:
        dbutils:   Databricks dbutils object (passed from notebook context)
        path:      Root Volume path to search recursively
        extension: File extension filter (e.g. ".csv", ".json")

    Returns:
        Tuple of (file_path, modification_time_ms)

    Raises:
        RuntimeError: If no matching files are found
    """
    files: list[tuple[str, int]] = []

    def _ls(p: str) -> None:
        for f in dbutils.fs.ls(p):
            if f.isDir():
                _ls(f.path)
            elif f.path.endswith(extension):
                files.append((f.path, f.modificationTime))

    _ls(path)

    if not files:
        raise RuntimeError(f"No {extension} files found recursively under {path}")

    latest_path, latest_mod_time = max(files, key=lambda f: f[1])
    return latest_path, latest_mod_time


def ingest_source_to_bronze(
    spark: SparkSession,
    source_cfg: dict[str, Any],
    pipeline_run_id: str,
    audit_table: str,
    dbutils: Any = None,
) -> None:
    """Load raw files from Volume to Bronze Delta table.

    Routes to append (Auto Loader streaming) or overwrite (batch latest file)
    based on source_cfg['load_mode'].

    Args:
        spark:           Active SparkSession
        source_cfg:      Validated source config dict from load_config()
        pipeline_run_id: Shared identifier across all sources in one run
        audit_table:     Fully qualified audit Delta table name
        dbutils:         Databricks dbutils (required for overwrite mode)

    Raises:
        Exception: Re-raises any failure after writing FAILED audit record
    """
    load_mode = source_cfg.get("load_mode", "append")
    has_flatten = bool(source_cfg.get("flatten"))

    if load_mode == "append" and has_flatten:
        _ingest_append_autoloader_flatten(spark, source_cfg, pipeline_run_id, audit_table, dbutils)
    elif load_mode == "append":
        _ingest_append(spark, source_cfg, pipeline_run_id, audit_table)
    elif load_mode == "overwrite":
        _ingest_overwrite(spark, source_cfg, pipeline_run_id, audit_table, dbutils)
    elif load_mode == "copy_into":
        _ingest_copy_into(spark, source_cfg, pipeline_run_id, audit_table)
    else:
        raise ValueError(f"Unknown load_mode: '{load_mode}'")


# ---------------------------------------------------------------------------
# Append — Auto Loader streaming, availableNow trigger
# ---------------------------------------------------------------------------

def _ingest_append(
    spark: SparkSession,
    source_cfg: dict[str, Any],
    pipeline_run_id: str,
    audit_table: str,
) -> None:
    """Auto Loader streaming ingestion — append-only, incremental.

    Uses availableNow=True trigger: processes all new files since last
    checkpoint, then exits. Behaves like a batch job despite using
    Structured Streaming internally.
    """
    source_name         = source_cfg["source_name"]
    file_format         = source_cfg["file_format"]
    input_path          = source_cfg["input_path"]
    target_table        = source_cfg["target_table"]
    schema_location     = source_cfg["schema_location"]
    checkpoint_location = source_cfg["checkpoint_location"]
    reader_options      = source_cfg.get("reader_options", {})

    audit_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)

    print(f"[{source_name}] append → {target_table}")

    try:
        df = _build_streaming_reader(
            spark, file_format, input_path, schema_location, reader_options,
            has_flatten=False,
        )

        df = _add_technical_columns(df, source_name)

        query = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target_table)
        )
        query.awaitTermination()

        end_ts   = datetime.now(UTC)
        progress = _extract_progress(query)

        print(
            f"[{source_name}] SUCCESS — "
            f"files={progress['files_processed']} "
            f"rows={progress['rows_written']} "
            f"elapsed={round((end_ts - start_ts).total_seconds(), 2)}s"
        )

        write_audit_record(spark, {
            "audit_id":            audit_id,
            "pipeline_run_id":     pipeline_run_id,
            "source_name":         source_name,
            "target_table":        target_table,
            "input_path":          input_path,
            "checkpoint_location": checkpoint_location,
            "schema_location":     schema_location,
            "start_ts":            start_ts,
            "end_ts":              end_ts,
            "status":              "SUCCESS",
            "files_processed":     progress["files_processed"],
            "rows_written":        progress["rows_written"],
            "error_message":       None,
        }, audit_table)

    except Exception as exc:
        _write_failed_audit(
            spark, audit_id, pipeline_run_id, source_cfg, start_ts, exc, audit_table
        )
        raise


# ---------------------------------------------------------------------------
# Append + flatten — Auto Loader streaming with foreachBatch
# ---------------------------------------------------------------------------

def _ingest_append_autoloader_flatten(
    spark: SparkSession,
    source_cfg: dict[str, Any],
    pipeline_run_id: str,
    audit_table: str,
    dbutils: Any = None,
) -> None:
    """Auto Loader streaming ingestion with flatten applied inside foreachBatch.

    Uses an availableNow trigger to process files discovered since the previous
    checkpoint and then stop. Databricks documents foreachBatch as at-least-once:
    a failure after the append but before checkpoint commit can replay a micro-batch.
    Downstream transformations must therefore use stable business keys when they
    require deduplication.
    """
    source_name         = source_cfg["source_name"]
    file_format         = source_cfg["file_format"]
    input_path          = source_cfg["input_path"]
    target_table        = source_cfg["target_table"]
    schema_location     = source_cfg["schema_location"]
    checkpoint_location = source_cfg["checkpoint_location"]
    flatten_cfg         = source_cfg["flatten"]
    reader_options      = source_cfg.get("reader_options", {})

    audit_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)

    print(f"[{source_name}] append → {target_table}")

    output_cols = _get_flatten_output_columns(flatten_cfg)

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Flatten and write one micro-batch to Delta.

        Runs in a Spark Connect cloned session (separate OS process).
        Mutations to outer-scope Python variables are not visible in the main process
        after awaitTermination() — do not use shared state for metrics or flags.
        """
        if batch_df.isEmpty():
            return

        # Extract file metadata BEFORE flatten/explode — _metadata was aliased to
        # _file_metadata in readStream. Stored as tmp columns so flatten can propagate
        # them through its select() expressions without calling df.columns.
        batch_df = batch_df.withColumn(
            "_file_path_tmp", F.col("_file_metadata.file_path")
        ).withColumn(
            "_file_mod_time_tmp", F.col("_file_metadata.file_modification_time")
        )

        flat_df = apply_flatten(batch_df, flatten_cfg)
        flat_df = _add_technical_columns_batch(
            flat_df,
            source_name=source_name,
            source_file_path=None,
            source_file_mod_time=None,
            column_names=output_cols,
            use_input_file_name=False,
        )
        # Replace NULL literals set by _add_technical_columns_batch with per-row
        # values preserved before explode via the passthrough pattern.
        flat_df = flat_df \
            .withColumn("_source_file_path", F.col("_file_path_tmp")) \
            .withColumn("_source_file_name",
                        F.element_at(F.split(F.col("_file_path_tmp"), "/"), -1)) \
            .withColumn("_source_file_modification_time", F.col("_file_mod_time_tmp")) \
            .drop("_file_path_tmp", "_file_mod_time_tmp")

        row_count = flat_df.count()
        print(f"[{source_name}] batch={batch_id} rows={row_count}")

        flat_df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .saveAsTable(target_table)

    try:
        df = _build_streaming_reader(
            spark, file_format, input_path, schema_location, reader_options,
            has_flatten=True,
            schema_hints=source_cfg.get("schema_hints", ""),
        )
        # Select _metadata before foreachBatch — per Databricks docs:
        # "If you only reference the file metadata column inside the foreachBatch
        # function, the column is not included." Alias avoids collision with source
        # data columns that may also be named _metadata.
        df = df.select("*", F.col("_metadata").alias("_file_metadata"))

        query = (
            df.writeStream
            .foreachBatch(_process_batch)
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

        progress = _extract_progress(query)
        files_discovered = progress["files_processed"] or 0

        end_ts  = datetime.now(UTC)
        elapsed = round((end_ts - start_ts).total_seconds(), 2)

        # Row count cannot be read from _batch_stats: foreachBatch runs in a
        # Spark Connect cloned session (separate process) — mutations to shared
        # Python dicts in the outer scope are never propagated back.
        # Read from Delta transaction log instead — zero data scan.
        rows = _rows_written_from_history(spark, target_table, start_ts)

        if rows == 0:
            print(f"[{source_name}] no new data elapsed={elapsed}s")
        else:
            print(f"[{source_name}] SUCCESS rows={rows} elapsed={elapsed}s")

        _write_success_audit(spark, audit_id, pipeline_run_id, source_cfg,
                             start_ts, files_discovered, rows, audit_table)

    except Exception as exc:
        _write_failed_audit(
            spark, audit_id, pipeline_run_id, source_cfg, start_ts, exc, audit_table
        )
        raise


def _load_last_processed_file(dbutils: Any, checkpoint_file: str) -> str | None:
    """Return path of last successfully processed file for overwrite sources."""
    try:
        return dbutils.fs.head(checkpoint_file, 10_000).strip() or None
    except Exception:
        return None


def _save_last_processed_file(dbutils: Any, checkpoint_file: str, path: str) -> None:
    """Persist path of last successfully processed file for overwrite sources."""
    dbutils.fs.put(checkpoint_file, path, overwrite=True)


def _write_success_audit(
    spark: SparkSession,
    audit_id: str,
    pipeline_run_id: str,
    source_cfg: dict[str, Any],
    start_ts: datetime,
    files_processed: int,
    rows_written: int,
    audit_table: str,
) -> None:
    write_audit_record(spark, {
        "audit_id":            audit_id,
        "pipeline_run_id":     pipeline_run_id,
        "source_name":         source_cfg.get("source_name"),
        "target_table":        source_cfg.get("target_table"),
        "input_path":          source_cfg.get("input_path"),
        "checkpoint_location": source_cfg.get("checkpoint_location"),
        "schema_location":     source_cfg.get("schema_location"),
        "start_ts":            start_ts,
        "end_ts":              datetime.now(UTC),
        "status":              "SUCCESS",
        "files_processed":     files_processed,
        "rows_written":        rows_written,
        "error_message":       None,
    }, audit_table)


# ---------------------------------------------------------------------------
# Copy Into — idempotent append for reference data (CSV)
# ---------------------------------------------------------------------------

def _ingest_copy_into(
    spark: SparkSession,
    source_cfg: dict[str, Any],
    pipeline_run_id: str,
    audit_table: str,
) -> None:
    """COPY INTO ingestion — idempotent append, state managed by Delta log.

    Skips already-loaded files automatically — no custom checkpoint needed.
    Each new file version is appended as a new partition (_bronze_ingestion_date).
    Designed for reference data (e.g. OurAirports CSV) that changes infrequently.

    The target table must be pre-created with PARTITIONED BY (_bronze_ingestion_date)
    — see sql/ddl/030_create_our_airports_bronze_tables.sql.
    """
    source_name    = source_cfg["source_name"]
    input_path     = source_cfg["input_path"]
    target_table   = source_cfg["target_table"]
    reader_options = source_cfg.get("reader_options", {})
    header         = reader_options.get("header", "true")

    audit_id  = str(uuid.uuid4())
    start_ts  = datetime.now(UTC)
    version_before = _current_table_version(spark, target_table)

    print(f"[{source_name}] copy_into → {target_table}")

    try:
        spark.sql(f"""
            COPY INTO {target_table}
            FROM (
                SELECT *,
                    current_date()      AS _bronze_ingestion_date,
                    current_timestamp() AS _bronze_ingest_ts,
                    '{source_name}'     AS _bronze_source,
                    _metadata.file_path AS _source_file_path
                FROM '{input_path}'
            )
            FILEFORMAT = CSV
            FORMAT_OPTIONS ('header' = '{header}', 'inferSchema' = 'false')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """)

        version_after = _current_table_version(spark, target_table)
        end_ts  = datetime.now(UTC)
        elapsed = round((end_ts - start_ts).total_seconds(), 2)

        if version_after == version_before:
            # COPY INTO found no new files — table version unchanged
            print(f"[{source_name}] SKIPPED — no new files elapsed={elapsed}s")
            _write_success_audit(spark, audit_id, pipeline_run_id, source_cfg,
                                 start_ts, 0, 0, audit_table)
        else:
            rows_written = _rows_written_from_history(spark, target_table)
            print(f"[{source_name}] SUCCESS — rows={rows_written} elapsed={elapsed}s")
            _write_success_audit(spark, audit_id, pipeline_run_id, source_cfg,
                                 start_ts, 1, rows_written, audit_table)

    except Exception as exc:
        _write_failed_audit(
            spark, audit_id, pipeline_run_id, source_cfg, start_ts, exc, audit_table
        )
        raise


def _ingest_overwrite(
    spark: SparkSession,
    source_cfg: dict[str, Any],
    pipeline_run_id: str,
    audit_table: str,
    dbutils: Any = None,
) -> None:
    """Batch ingestion — reads newest file only, overwrites target table.

    Uses dbutils.fs.ls() to find the latest file recursively — this is the
    recommended approach for UC Volumes as it uses the FUSE layer directly
    from the driver, unlike binaryFile which uses Hadoop FileSystem APIs.
    """
    source_name    = source_cfg["source_name"]
    file_format    = source_cfg["file_format"]
    input_path     = source_cfg["input_path"]
    target_table   = source_cfg["target_table"]
    reader_options = source_cfg.get("reader_options", {})

    audit_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)

    print(f"[{source_name}] overwrite → {target_table}")

    try:
        _BATCH_READER_SKIP_OPTIONS = {"pathGlobFilter", "inferSchema", "recursiveFileLookup"}

        # Find the single newest file using dbutils.fs.ls() (UC Volumes recommended)
        extension   = f".{file_format}"
        latest_path, latest_mod_time = _find_latest_file(dbutils, input_path, extension)
        print(f"[{source_name}] latest file: {latest_path}")

        # Skip if the same file was already processed in a previous run
        checkpoint_location = source_cfg.get("checkpoint_location", "")
        last_file_checkpoint = checkpoint_location.rstrip("/") + "/_last_processed_file.txt"
        last_processed = _load_last_processed_file(dbutils, last_file_checkpoint)

        if last_processed == latest_path:
            print(f"[{source_name}] file unchanged since last run, skipping")
            _write_success_audit(spark, audit_id, pipeline_run_id, source_cfg,
                                 start_ts, 0, 0, audit_table)
            return

        # Read only the newest file
        if file_format == "csv":
            header     = reader_options.get("header", "true").lower() == "true"
            extra_opts = {k: v for k, v in reader_options.items() if k not in _BATCH_READER_SKIP_OPTIONS}
            reader = (
                spark.read
                .option("header", header)
                .option("inferSchema", False)
            )
            for k, v in extra_opts.items():
                reader = reader.option(k, v)
            df = reader.csv(latest_path)
        else:
            # For non-CSV formats: also use dbutils.fs.ls() for consistency
            latest_path, latest_mod_time = _find_latest_file(dbutils, input_path, f".{file_format}")
            print(f"[{source_name}] latest file: {latest_path}")
            reader = spark.read.format(file_format)
            for key, value in reader_options.items():
                if key not in _BATCH_READER_SKIP_OPTIONS:
                    reader = reader.option(key, value)
            df = reader.load(latest_path)

        # Step 3: apply flatten if configured
        flatten_cfg = source_cfg.get("flatten")
        if flatten_cfg:
            df = apply_flatten(df, flatten_cfg)
            print(f"[{source_name}] flatten applied → {len(df.columns)} columns")

        # Step 4: add technical columns (batch variant — no _metadata struct)
        df = _add_technical_columns_batch(
            df,
            source_name=source_name,
            source_file_path=latest_path,
            source_file_mod_time=latest_mod_time,
        )

        # Step 4: overwrite target table
        # overwriteSchema=true needed when column types change (e.g. inferSchema off → all StringType)
        (
            df.write
            .format("delta")
            .option("overwriteSchema", "true")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        # Persist the processed file path — next run will skip if file hasn't changed
        _save_last_processed_file(dbutils, last_file_checkpoint, latest_path)

        # Count rows written from Delta transaction log — reads single JSON entry,
        # no data scan. Works for both overwrite and append.
        rows_written = _rows_written_from_history(spark, target_table)

        end_ts = datetime.now(UTC)
        print(
            f"[{source_name}] SUCCESS — "
            f"rows={rows_written} "
            f"elapsed={round((end_ts - start_ts).total_seconds(), 2)}s"
        )

        write_audit_record(spark, {
            "audit_id":            audit_id,
            "pipeline_run_id":     pipeline_run_id,
            "source_name":         source_name,
            "target_table":        target_table,
            "input_path":          input_path,
            "checkpoint_location": source_cfg.get("checkpoint_location"),
            "schema_location":     source_cfg.get("schema_location"),
            "start_ts":            start_ts,
            "end_ts":              end_ts,
            "status":              "SUCCESS",
            "files_processed":     1,
            "rows_written":        rows_written,
            "error_message":       None,
        }, audit_table)

    except Exception as exc:
        _write_failed_audit(
            spark, audit_id, pipeline_run_id, source_cfg, start_ts, exc, audit_table
        )
        raise


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _get_flatten_output_columns(flatten_cfg: dict) -> list[str]:
    """Return list of output column names from flatten config — no schema fetch.

    Used to pass explicit column_names to _add_technical_columns_batch
    inside foreachBatch, avoiding any .columns call on a lazy Spark Connect DF.
    """
    from bronze_ingestion.opensky_schema import OPENSKY_STATE_FIELDS as _OPENSKY_STATE_FIELDS

    cols: list[str] = list(flatten_cfg.get("top_level_columns", []))
    mode = flatten_cfg.get("mode")

    if mode == "opensky_states":
        cols += [name for name, _ in _OPENSKY_STATE_FIELDS]
        return cols

    # scalar fields
    cols += list(flatten_cfg.get("scalar_fields", []))

    # nested_fields → parent_field
    for parent, fields in flatten_cfg.get("nested_fields", {}).items():
        cols += [f"{parent}_{f}" for f in fields]

    # array_first_fields → parent_field
    for parent, fields in flatten_cfg.get("array_first_fields", {}).items():
        cols += [f"{parent}_{f}" for f in fields]

    # keep_as_struct
    cols += list(flatten_cfg.get("keep_as_struct", []))

    # city_fields → city_field
    cols += [f"city_{f}" for f in flatten_cfg.get("city_fields", [])]

    # multi-explode: add direction_column + flight_date + flight_status
    if flatten_cfg.get("explode_columns"):
        direction_col = flatten_cfg.get("direction_column", "flight_direction")
        cols = [direction_col] + cols + ["flight_date", "flight_status"]

    return cols

def _build_streaming_reader(
    spark: SparkSession,
    file_format: str,
    input_path: str,
    schema_location: str,
    reader_options: dict[str, str],
    has_flatten: bool = False,
    schema_hints: str = "",
) -> DataFrame:
    """Configure and return an Auto Loader streaming DataFrame.

    `schema_hints`: cloudFiles.schemaHints string to force correct types for complex columns.
    Auto Loader defaults all JSON columns to STRING; hints override this on initial schema
    inference. Hints are ignored if schema_location already contains a cached schema —
    delete the schema_location directory to force re-inference with updated hints.

    `schemaEvolutionMode` is always 'rescue': unknown fields (including new fields inside
    existing ARRAY<STRUCT<...>>) go to _rescued_data instead of crashing the stream.
    addNewColumns is not used because it can only add new top-level columns, not new fields
    inside nested structs, which is the common evolution pattern for aviationstack/awc/owm.
    """
    evolution_mode = "rescue"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", evolution_mode)
        .option("cloudFiles.includeExistingFiles", "true")
    )
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)
    if not has_flatten:
        reader = reader.option("rescuedDataColumn", "_rescued_data")

    for key, value in reader_options.items():
        reader = reader.option(key, value)
    return reader.load(input_path)


def _add_technical_columns(df: DataFrame, source_name: str) -> DataFrame:
    """Add Bronze technical columns to streaming DataFrame.

    Uses _metadata struct (available in Auto Loader) for file-level info.
    Excludes _metadata and _rescued_data from record hash.
    """
    source_cols = [
        c for c in df.columns
        if c not in {"_metadata", "_rescued_data"}
    ]

    hash_expr = F.md5(
        F.concat_ws("|", *[F.col(c).cast("string") for c in source_cols])
    )

    return (
        df
        .withColumn("_bronze_ingest_ts",             F.current_timestamp())
        .withColumn("_bronze_source",                F.lit(source_name))
        .withColumn("_bronze_ingestion_date",         F.to_date(F.current_timestamp()))
        .withColumn("_source_file_path",              F.col("_metadata.file_path"))
        .withColumn("_source_file_name",              F.col("_metadata.file_name"))
        .withColumn("_source_file_size",              F.col("_metadata.file_size"))
        .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("_bronze_record_hash",            hash_expr)
        .drop("_metadata")
    )


def _sanitize_name(col: str) -> str:
    """Return column name with invalid Delta Lake characters replaced by underscore."""
    _INVALID = re.compile(r'[ ,;{}\(\)\n\t=]')
    return _INVALID.sub("_", col)


def _sanitize_column_names(df: DataFrame) -> DataFrame:
    """Rename columns containing characters invalid for Delta Lake.

    Delta table column names must not contain: space , ; { } ( ) \n \t =
    Replaces any such character with underscore. Deduplicates if needed.
    """
    _INVALID = re.compile(r'[ ,;{}\(\)\n\t=]')
    seen: dict[str, int] = {}
    for col in df.columns:
        safe = _INVALID.sub("_", col)
        if safe in seen:
            seen[safe] += 1
            safe = f"{safe}_{seen[safe]}"
        else:
            seen[safe] = 0
        if safe != col:
            df = df.withColumnRenamed(col, safe)
    return df


def _sanitize_column_names_explicit(df: DataFrame, columns: list[str]) -> DataFrame:
    """Rename only the provided columns — no schema fetch needed."""
    for col in columns:
        safe = _sanitize_name(col)
        if safe != col:
            df = df.withColumnRenamed(col, safe)
    return df


def _add_technical_columns_batch(
    df: DataFrame,
    source_name: str,
    source_file_path: str | None,
    source_file_mod_time: Any,
    column_names: list[str] | None = None,
    use_input_file_name: bool = False,
) -> DataFrame:
    """Add Bronze technical columns to a batch DataFrame.

    `column_names`: explicit column list used to compute the record hash without
    calling df.columns — necessary in foreachBatch (Spark Connect gRPC constraint).
    If None, df.columns is used — only safe in a plain batch (non-streaming) context.

    `use_input_file_name`: kept for API compatibility but should not be used in UC
    foreachBatch context — Unity Catalog blocks input_file_name() there. Populate
    _source_file_path via the _file_path_tmp passthrough pattern instead.
    """
    if column_names is not None:
        # Derive hash from explicit column list — no df.columns call needed.
        # Required in foreachBatch: df.columns triggers a gRPC schema() round-trip
        # which fails when executed inside a Spark Connect cloned session.
        df = _sanitize_column_names_explicit(df, column_names)
        safe_names = [_sanitize_name(c) for c in column_names]
        hash_expr = F.md5(
            F.concat_ws("|", *[F.col(c).cast("string") for c in safe_names])
        )
    else:
        # Fallback: hash all columns via df.columns — safe only in batch (non-foreachBatch) context.
        df = _sanitize_column_names(df)
        source_cols = list(df.columns)
        hash_expr = F.md5(
            F.concat_ws("|", *[F.col(c).cast("string") for c in source_cols])
        )

    if use_input_file_name:
        file_path_col = F.input_file_name()
        file_name_col = F.element_at(F.split(F.input_file_name(), "/"), -1)
    else:
        file_path_col = F.lit(source_file_path)
        file_name_col = F.lit(source_file_path.split("/")[-1] if source_file_path else None)

    return (
        df
        .withColumn("_bronze_ingest_ts",             F.current_timestamp())
        .withColumn("_bronze_source",                F.lit(source_name))
        .withColumn("_bronze_ingestion_date",         F.to_date(F.current_timestamp()))
        .withColumn("_source_file_path",              file_path_col)
        .withColumn("_source_file_name",              file_name_col)
        .withColumn("_source_file_size",              F.lit(None).cast("long"))
        .withColumn("_source_file_modification_time", F.lit(source_file_mod_time).cast("timestamp"))
        .withColumn("_bronze_record_hash",            hash_expr)
    )


def _current_table_version(spark: SparkSession, target_table: str) -> int:
    """Return current Delta table version number.

    Used to detect whether COPY INTO actually wrote new data by comparing
    version before and after the operation. Zero data scan — reads only
    the Delta transaction log.
    """
    try:
        history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").first()
        return int(history["version"])
    except Exception:
        return -1


def _rows_written_from_history(
    spark: SparkSession, target_table: str, since_ts: datetime | None = None
) -> int:
    """Return rows written to target_table in the most recent Delta WRITE operation.

    Reads operationMetrics from DESCRIBE HISTORY LIMIT 1 (transaction log only — no data scan).
    When since_ts is provided, returns 0 if the last WRITE predates since_ts, meaning
    no data was written in this pipeline run.

    Delta history timestamps are timezone-naive UTC. since_ts may be tz-aware (datetime.now(utc))
    — strip tzinfo before comparison to avoid TypeError.
    """
    try:
        history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").first()
        if history is None:
            return 0
        op_ts = history["timestamp"]  # naive UTC datetime from Delta transaction log
        if op_ts is None:
            return 0
        if since_ts is not None:
            # Delta history timestamps are naive UTC; since_ts from datetime.now(timezone.utc)
            # is tz-aware — strip tzinfo before comparison.
            since_naive = since_ts.replace(tzinfo=None)
            if op_ts < since_naive:
                return 0
        return int((history["operationMetrics"] or {}).get("numOutputRows", 0))
    except Exception:
        return 0


def _extract_progress(query) -> dict[str, int | None]:
    """Extract file and row metrics from completed streaming query progress.

    Aggregates across all micro-batches in recentProgress — availableNow=True
    may process multiple micro-batches before terminating.

    numFilesDiscovered counts files discovered in the current trigger window only.
    Files processed via Auto Loader backfill (from a previous trigger) report 0 —
    this metric is informational and should not be used for correctness checks.

    Returns None for each metric on any error — metrics are best-effort only.
    """
    try:
        all_progress = query.recentProgress
        if not all_progress:
            progress = query.lastProgress
            if progress is None:
                return {"files_processed": None, "rows_written": None}
            all_progress = [progress]

        rows_written = sum(
            int(p.get("numInputRows") or 0)
            for p in all_progress
        )
        # numFilesDiscovered = total files seen by Auto Loader in this trigger
        files_processed = sum(
            int(p.get("sources", [{}])[0].get("metrics", {}).get("numFilesDiscovered") or 0)
            for p in all_progress
        )

        return {
            "files_processed": files_processed,
            "rows_written":    rows_written,
        }
    except Exception:
        # Swallowed intentionally — progress metrics are informational, not operational.
        return {"files_processed": None, "rows_written": None}


def _write_failed_audit(
    spark: SparkSession,
    audit_id: str,
    pipeline_run_id: str,
    source_cfg: dict[str, Any],
    start_ts: datetime,
    exc: Exception,
    audit_table: str,
) -> None:
    """Write FAILED audit record. Best-effort — logs warning on audit failure."""
    try:
        write_audit_record(spark, {
            "audit_id":            audit_id,
            "pipeline_run_id":     pipeline_run_id,
            "source_name":         source_cfg.get("source_name"),
            "target_table":        source_cfg.get("target_table"),
            "input_path":          source_cfg.get("input_path"),
            "checkpoint_location": source_cfg.get("checkpoint_location"),
            "schema_location":     source_cfg.get("schema_location"),
            "start_ts":            start_ts,
            "end_ts":              datetime.now(UTC),
            "status":              "FAILED",
            "files_processed":     None,
            "rows_written":        None,
            "error_message":       sanitize_exception(exc),
        }, audit_table)
    except Exception as audit_exc:
        print(
            "WARNING: failed to write FAILED audit record: "
            f"{sanitize_exception(audit_exc)}"
        )
