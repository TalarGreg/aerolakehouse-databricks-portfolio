from bronze_ingestion.opensky_schema import (
    OPENSKY_API_FIELD_COUNT,
    OPENSKY_INGEST_FIELDS,
    OPENSKY_SENSORS_INDEX,
    OPENSKY_STATE_FIELDS,
)


def test_opensky_schema_matches_extended_api_contract() -> None:
    field_names = [name for name, _ in OPENSKY_STATE_FIELDS]

    assert OPENSKY_API_FIELD_COUNT == 18
    assert len(field_names) == OPENSKY_API_FIELD_COUNT
    assert field_names[OPENSKY_SENSORS_INDEX] == "sensors"
    assert field_names[-1] == "category"
    assert len(field_names) == len(set(field_names))


def test_landing_mapping_excludes_only_sensors() -> None:
    canonical_names = [name for name, _ in OPENSKY_STATE_FIELDS]

    assert "sensors" not in OPENSKY_INGEST_FIELDS
    assert OPENSKY_INGEST_FIELDS == [
        name for name in canonical_names if name != "sensors"
    ]
