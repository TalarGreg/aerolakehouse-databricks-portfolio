from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bronze_ingestion.opensky_schema import OPENSKY_STATE_FIELDS as _OPENSKY_STATE_FIELDS


def _add_passthrough_exprs(
    select_exprs: list,
    passthrough: list[str] | None,
) -> None:
    """Append passthrough columns to select_exprs unconditionally.

    Used to carry _file_path_tmp / _file_mod_time_tmp through flatten/select
    so that foreachBatch can populate _source_file_path after transformation.

    NOTE: In Spark Connect, df.columns triggers a gRPC schema() round-trip which
    fails inside foreachBatch. We therefore append passthrough columns unconditionally
    — the caller (apply_flatten) only passes them when the column was explicitly added
    to the DF before calling apply_flatten, so they are guaranteed to exist.
    For plain batch reads apply_flatten is called without passthrough (None), so
    this function is a no-op in that path.
    """
    if not passthrough:
        return
    for col_name in passthrough:
        select_exprs.append(F.col(col_name))


def apply_flatten(df: DataFrame, flatten_cfg: dict[str, Any]) -> DataFrame:
    """Apply flattening transformation to a Bronze DataFrame based on config.

    Supports four modes detected from config keys:
    - explode_columns:  union of multiple exploded arrays (Aviationstack)
    - explode_column:   single array explode (AWC, OWM forecast)
    - opensky_states:   OpenSky states stored as array<struct>
    - single_record:    struct extraction without explode (OWM current)

    NOTE: _flatten_opensky_states uses named struct access and explicit casts. No
    df.schema call is made inside foreachBatch, which is required for Spark Connect
    compatibility. All modes derive column references from static definitions or YAML.

    _file_path_tmp and _file_mod_time_tmp are pass-through columns — if present on the
    input DF (injected by foreachBatch before apply_flatten), they are carried through
    all select_exprs so that _source_file_path can be populated after flatten.
    """
    _PASSTHROUGH = ["_file_path_tmp", "_file_mod_time_tmp"]

    if flatten_cfg.get("explode_columns"):
        return _flatten_multi_explode(df, flatten_cfg, _PASSTHROUGH)
    if flatten_cfg.get("mode") == "opensky_states":
        return _flatten_opensky_states(df, flatten_cfg, _PASSTHROUGH)
    if flatten_cfg.get("explode_column"):
        return _flatten_single_explode(df, flatten_cfg, _PASSTHROUGH)
    if flatten_cfg.get("mode") == "single_record":
        return _flatten_single_record(df, flatten_cfg, _PASSTHROUGH)
    raise ValueError(f"Cannot detect flatten mode from config: {flatten_cfg}")


# ---------------------------------------------------------------------------
# Mode: opensky_states — named structs mapped from the positional source API
# ---------------------------------------------------------------------------

def _flatten_opensky_states(df: DataFrame, cfg: dict[str, Any],
                             passthrough: list[str] | None = None) -> DataFrame:
    """Parse OpenSky states into flat rows.

    Expects Format A — named dicts produced by map_states() in bronze_opensky_ingest.py.
    Auto Loader infers states as ARRAY<STRUCT<...>> from JSON files (enforced via schema_hints).
    After explode, each field is accessed by name and cast to its canonical type.

    Fields defined in OPENSKY_STATE_FIELDS but absent from the struct (e.g. sensors on
    non-feeder accounts, category on older files) resolve to NULL — no schema fetch needed.
    """
    top_level_cols = cfg.get("top_level_columns", [])

    df = df.withColumn("_state_raw", F.explode(F.col("states")))

    select_exprs = [F.col(c) for c in top_level_cols]
    for field_name, field_type in _OPENSKY_STATE_FIELDS:
        # Accessing a missing struct field returns NULL — no exception, no schema check.
        select_exprs.append(
            F.col(f"_state_raw.{field_name}").cast(field_type).alias(field_name)
        )
    _add_passthrough_exprs(select_exprs, passthrough)
    return df.select(*select_exprs)


# ---------------------------------------------------------------------------
# Mode: single explode — AWC data, OWM forecast
# ---------------------------------------------------------------------------

def _flatten_single_explode(df: DataFrame, cfg: dict[str, Any],
                             passthrough: list[str] | None = None) -> DataFrame:
    """Explode one array column, select top-level and nested fields from config."""
    explode_col    = cfg["explode_column"]
    top_level_cols = cfg.get("top_level_columns", [])
    nested_fields  = cfg.get("nested_fields", {})
    array_first    = cfg.get("array_first_fields", {})
    scalar_fields  = cfg.get("scalar_fields", [])
    keep_as_struct = cfg.get("keep_as_struct", [])
    city_fields    = cfg.get("city_fields", [])

    df = df.withColumn("_exploded", F.explode(F.col(explode_col)))

    select_exprs = []

    # Root document metadata columns
    for col in top_level_cols:
        select_exprs.append(F.col(col))

    # Scalar fields directly inside exploded struct
    for field in scalar_fields:
        select_exprs.append(F.col(f"_exploded.{field}").alias(field))

    # Nested struct fields: exploded.parent.field → parent_field
    for parent, fields in nested_fields.items():
        for field in fields:
            select_exprs.append(
                F.col(f"_exploded.{parent}.{field}").alias(f"{parent}_{field}")
            )

    # First element of array fields: exploded.parent[0].field → parent_field
    for parent, fields in array_first.items():
        for field in fields:
            select_exprs.append(
                F.col(f"_exploded.{parent}")[0][field].alias(f"{parent}_{field}")
            )

    # Keep struct columns as-is (e.g. coords in AWC)
    for col in keep_as_struct:
        select_exprs.append(F.col(f"_exploded.{col}").alias(col))

    # City struct at root level (OWM specific)
    for field in city_fields:
        select_exprs.append(F.col(f"city.{field}").alias(f"city_{field}"))

    _add_passthrough_exprs(select_exprs, passthrough)
    return df.select(*select_exprs)


# ---------------------------------------------------------------------------
# Mode: multi explode — Aviationstack (departures + arrivals → union)
# ---------------------------------------------------------------------------

def _flatten_multi_explode(df: DataFrame, cfg: dict[str, Any],
                            passthrough: list[str] | None = None) -> DataFrame:
    """Explode multiple array columns and union with a direction discriminator.

    allowMissingColumns=True in unionByName is required because departure.* and
    arrival.* sub-structs have different field sets — union would fail without it.
    """
    explode_columns = cfg["explode_columns"]
    direction_col   = cfg.get("direction_column", "flight_direction")
    top_level_cols  = cfg.get("top_level_columns", [])
    nested_fields   = cfg.get("nested_fields", {})

    dfs = []
    for spec in explode_columns:
        col_name  = spec["column"]
        direction = spec["direction_value"]

        exploded = df.withColumn("_exploded", F.explode(F.col(col_name)))

        select_exprs = [F.lit(direction).alias(direction_col)]

        for col in top_level_cols:
            select_exprs.append(F.col(col))

        for field in ["flight_date", "flight_status"]:
            select_exprs.append(F.col(f"_exploded.{field}").alias(field))

        for parent, fields in nested_fields.items():
            for field in fields:
                select_exprs.append(
                    F.col(f"_exploded.{parent}.{field}").alias(f"{parent}_{field}")
                )

        _add_passthrough_exprs(select_exprs, passthrough)
        dfs.append(exploded.select(*select_exprs))

    result = dfs[0]
    for other in dfs[1:]:
        result = result.unionByName(other, allowMissingColumns=True)

    return result


# ---------------------------------------------------------------------------
# Mode: single record — OWM current (no explode, struct extraction)
# ---------------------------------------------------------------------------

def _flatten_single_record(df: DataFrame, cfg: dict[str, Any],
                            passthrough: list[str] | None = None) -> DataFrame:
    """Extract a single struct column to flat columns without exploding."""
    source_col     = cfg["source_column"]
    top_level_cols = cfg.get("top_level_columns", [])
    nested_fields  = cfg.get("nested_fields", {})
    array_first    = cfg.get("array_first_fields", {})
    scalar_fields  = cfg.get("scalar_fields", [])
    city_fields    = cfg.get("city_fields", [])

    select_exprs = []

    for col in top_level_cols:
        select_exprs.append(F.col(col))

    for field in scalar_fields:
        select_exprs.append(F.col(f"{source_col}.{field}").alias(field))

    for parent, fields in nested_fields.items():
        for field in fields:
            select_exprs.append(
                F.col(f"{source_col}.{parent}.{field}").alias(f"{parent}_{field}")
            )

    for parent, fields in array_first.items():
        for field in fields:
            select_exprs.append(
                F.col(f"{source_col}.{parent}")[0][field].alias(f"{parent}_{field}")
            )

    for field in city_fields:
        select_exprs.append(F.col(f"city.{field}").alias(f"city_{field}"))

    _add_passthrough_exprs(select_exprs, passthrough)
    return df.select(*select_exprs)
