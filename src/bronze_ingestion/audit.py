from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_SCHEMA = StructType([
    StructField("audit_id",            StringType(),    nullable=False),
    StructField("pipeline_run_id",     StringType(),    nullable=True),
    StructField("source_name",         StringType(),    nullable=True),
    StructField("target_table",        StringType(),    nullable=True),
    StructField("input_path",          StringType(),    nullable=True),
    StructField("checkpoint_location", StringType(),    nullable=True),
    StructField("schema_location",     StringType(),    nullable=True),
    StructField("start_ts",            TimestampType(), nullable=True),
    StructField("end_ts",              TimestampType(), nullable=True),
    StructField("status",              StringType(),    nullable=True),
    StructField("files_processed",     LongType(),      nullable=True),
    StructField("rows_written",        LongType(),      nullable=True),
    StructField("error_message",       StringType(),    nullable=True),
])


def write_audit_record(
    spark: SparkSession,
    audit_record: dict[str, Any],
    audit_table: str,
) -> None:
    """Write a single audit record to the Delta audit table.

    Args:
        spark:        Active SparkSession
        audit_record: Dict matching AUDIT_SCHEMA fields
        audit_table:  Fully qualified Delta table name

    Raises:
        Exception: Re-raises any write failure — never silently swallowed
    """
    row = {field.name: audit_record.get(field.name) for field in AUDIT_SCHEMA}
    df  = spark.createDataFrame([row], schema=AUDIT_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(audit_table)
