from pathlib import Path

import pytest

from bronze_ingestion.config import load_config

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _write_config(tmp_path: Path, body: str) -> Path:
    config_path = tmp_path / "bronze_sources.yml"
    config_path.write_text(body, encoding="utf-8")
    return config_path


def test_repository_config_loads_and_resolves_catalog() -> None:
    sources = load_config(
        str(REPOSITORY_ROOT / "configs" / "bronze_sources.yml"),
        catalog="ci_alh",
    )

    assert len(sources) == 9
    assert {source["source_name"] for source in sources} == {
        "opensky",
        "aviationstack",
        "open_weather_map_current",
        "open_weather_map_forecast",
        "awc_metar",
        "our_airports_airports",
        "our_airports_runways",
        "our_airports_countries",
        "our_airports_regions",
    }
    assert all("{catalog}" not in source["input_path"] for source in sources)
    assert all(source["target_table"].startswith("ci_alh.") for source in sources)

    awc_metar = next(source for source in sources if source["source_name"] == "awc_metar")
    assert awc_metar["input_path"].endswith("/awc/product=metar/")
    assert awc_metar["target_table"] == "ci_alh.bronze.raw_awc_metar"


@pytest.mark.parametrize("catalog", ["dev-alh", "dev.alh", "dev_alh; DROP CATALOG prod"])
def test_invalid_catalog_identifier_is_rejected(catalog: str) -> None:
    with pytest.raises(ValueError, match="valid Unity Catalog identifier"):
        load_config(
            str(REPOSITORY_ROOT / "configs" / "bronze_sources.yml"),
            catalog=catalog,
        )


def test_copy_into_source_does_not_require_checkpoint_paths(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
sources:
  - source_name: reference
    file_format: csv
    load_mode: copy_into
    input_path: /Volumes/{catalog}/bronze/raw_files/reference/
    target_table: "{catalog}.bronze.raw_reference"
""",
    )

    source = load_config(str(config_path), catalog="dev_alh")[0]

    assert source["input_path"].startswith("/Volumes/dev_alh/")
    assert source["reader_options"] == {}


def test_append_source_requires_checkpoint_and_schema_locations(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
sources:
  - source_name: events
    file_format: json
    load_mode: append
    input_path: /Volumes/{catalog}/bronze/raw_files/events/
    target_table: "{catalog}.bronze.raw_events"
""",
    )

    with pytest.raises(ValueError, match="schema_location"):
        load_config(str(config_path), catalog="dev_alh")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("file_format", "xml", "unsupported file_format"),
        ("load_mode", "merge", "unsupported load_mode"),
    ],
)
def test_invalid_source_contract_is_rejected(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    source = {
        "source_name": "events",
        "file_format": "json",
        "load_mode": "append",
        "input_path": "/Volumes/{catalog}/bronze/raw_files/events/",
        "target_table": "{catalog}.bronze.raw_events",
        "schema_location": "/Volumes/{catalog}/bronze/raw_files/_schemas/events",
        "checkpoint_location": "/Volumes/{catalog}/bronze/raw_files/_checkpoints/events",
    }
    source[field] = value

    config_path = tmp_path / "bronze_sources.yml"
    import yaml

    config_path.write_text(yaml.safe_dump({"sources": [source]}), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_config(str(config_path), catalog="dev_alh")
