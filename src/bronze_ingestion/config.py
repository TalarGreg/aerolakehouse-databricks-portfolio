from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

CATALOG_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REQUIRED_FIELDS = [
    "source_name",
    "file_format",
    "load_mode",
    "input_path",
    "target_table",
]

# schema_location and checkpoint_location are required for append mode (Auto Loader)
# but not for copy_into, which manages state internally via the Delta transaction log.
REQUIRED_FIELDS_CHECKPOINT = [
    "schema_location",
    "checkpoint_location",
]

ALLOWED_FORMATS    = {"json", "csv"}
ALLOWED_LOAD_MODES = {"append", "overwrite", "copy_into"}


def load_config(config_path: str, catalog: str) -> list[dict[str, Any]]:
    """Load and validate bronze source configs from YAML file.

    Substitutes {catalog} placeholder in all string values.

    Args:
        config_path: Absolute path to bronze_sources.yml
        catalog:     Unity Catalog name (e.g. dev_alh, prod_alh)

    Returns:
        List of validated source config dicts with {catalog} resolved

    Raises:
        FileNotFoundError: If config file does not exist
        ValueError:        If config is missing required fields or has invalid values
    """
    if not CATALOG_NAME_PATTERN.fullmatch(catalog):
        raise ValueError(
            "catalog must be a valid Unity Catalog identifier containing only "
            "letters, digits, and underscores"
        )

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict) or "sources" not in raw:
        raise ValueError("Config must have a top-level 'sources' key")

    sources = raw["sources"]
    if not isinstance(sources, list) or len(sources) == 0:
        raise ValueError("'sources' must be a non-empty list")

    validated = []
    for i, source in enumerate(sources):
        source = _substitute_catalog(source, catalog)
        source.setdefault("reader_options", {})
        source.setdefault("load_mode", "append")
        _validate_source(source, index=i)
        validated.append(source)

    return validated


def _substitute_catalog(source: dict, catalog: str) -> dict:
    """Replace {catalog} placeholder in all string values recursively."""
    result = {}
    for key, value in source.items():
        if isinstance(value, str):
            result[key] = value.replace("{catalog}", catalog)
        elif isinstance(value, dict):
            result[key] = {
                k: v.replace("{catalog}", catalog) if isinstance(v, str) else v
                for k, v in value.items()
            }
        else:
            result[key] = value
    return result


def _validate_source(source: dict, index: int) -> None:
    """Validate a single source config entry."""
    label = source.get("source_name", f"index {index}")

    for field in REQUIRED_FIELDS:
        if field not in source or not source[field]:
            raise ValueError(
                f"Source '{label}': missing or empty required field '{field}'"
            )

    # schema_location and checkpoint_location are required for append mode only
    if source.get("load_mode") != "copy_into":
        for field in REQUIRED_FIELDS_CHECKPOINT:
            if field not in source or not source[field]:
                raise ValueError(
                    f"Source '{label}': missing or empty required field '{field}'"
                )

    fmt = source["file_format"]
    if fmt not in ALLOWED_FORMATS:
        raise ValueError(
            f"Source '{label}': unsupported file_format '{fmt}'. "
            f"Allowed: {sorted(ALLOWED_FORMATS)}"
        )

    mode = source["load_mode"]
    if mode not in ALLOWED_LOAD_MODES:
        raise ValueError(
            f"Source '{label}': unsupported load_mode '{mode}'. "
            f"Allowed: {sorted(ALLOWED_LOAD_MODES)}"
        )
