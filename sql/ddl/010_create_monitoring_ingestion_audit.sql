-- Bronze ingestion audit table
-- Tracks every Auto Loader run: source, status, rows written, errors
--
-- Run once per catalog environment.
-- Override target_catalog for another environment before running the DDL.

DECLARE OR REPLACE VARIABLE target_catalog STRING DEFAULT 'dev_alh';

CREATE TABLE IF NOT EXISTS IDENTIFIER(target_catalog || '.monitoring.ingestion_audit') (
    audit_id                STRING    NOT NULL  COMMENT 'Unique ID for this audit record (UUID)',
    pipeline_run_id         STRING              COMMENT 'Shared ID across all sources in one notebook run',
    source_name             STRING              COMMENT 'Source name from bronze_sources.yml',
    target_table            STRING              COMMENT 'Fully qualified target Delta table',
    input_path              STRING              COMMENT 'Volume path read by Auto Loader',
    checkpoint_location     STRING              COMMENT 'Auto Loader checkpoint path',
    schema_location         STRING              COMMENT 'Auto Loader schema inference path',
    start_ts                TIMESTAMP           COMMENT 'Ingestion start time (UTC)',
    end_ts                  TIMESTAMP           COMMENT 'Ingestion end time (UTC)',
    status                  STRING              COMMENT 'SUCCESS or FAILED',
    files_processed         BIGINT              COMMENT 'Files processed in this run (best-effort)',
    rows_written            BIGINT              COMMENT 'Rows written to target table (best-effort)',
    error_message           STRING              COMMENT 'Error message if status=FAILED, else NULL'
)
USING DELTA
COMMENT 'Append-only audit log for Bronze ingestion runs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'false',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
