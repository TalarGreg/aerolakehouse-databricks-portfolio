from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_RESOURCE_PATH = (
    REPOSITORY_ROOT / "resources" / "pipelines" / "opensky_silver.pipeline.yml"
)
JOB_RESOURCE_PATH = (
    REPOSITORY_ROOT / "resources" / "jobs" / "bronze_ingestion_e2e.job.yml"
)
PIPELINE_SQL_PATH = (
    REPOSITORY_ROOT / "src" / "pipelines" / "silver" / "opensky_state.sql"
)


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_bundle_includes_pipeline_resources() -> None:
    bundle = _load_yaml(REPOSITORY_ROOT / "databricks.yml")

    assert "resources/pipelines/*.yml" in bundle["include"]


def test_opensky_pipeline_is_triggered_serverless_and_advanced() -> None:
    pipeline = _load_yaml(PIPELINE_RESOURCE_PATH)["resources"]["pipelines"][
        "opensky_silver"
    ]

    assert pipeline["catalog"] == "${var.catalog}"
    assert pipeline["schema"] == "silver"
    assert pipeline["serverless"] is True
    assert pipeline["continuous"] is False
    assert pipeline["edition"] == "ADVANCED"

    relative_sql_path = pipeline["libraries"][0]["file"]["path"]
    resolved_sql_path = (PIPELINE_RESOURCE_PATH.parent / relative_sql_path).resolve()
    assert resolved_sql_path == PIPELINE_SQL_PATH.resolve()
    assert resolved_sql_path.is_file()


def test_bronze_job_triggers_incremental_silver_update() -> None:
    job = _load_yaml(JOB_RESOURCE_PATH)["resources"]["jobs"][
        "bronze_ingestion_e2e"
    ]
    tasks = {task["task_key"]: task for task in job["tasks"]}

    refresh_task = tasks["refresh_opensky_silver"]
    assert refresh_task["depends_on"] == [
        {"task_key": "validate_bronze_freshness"}
    ]
    assert refresh_task["pipeline_task"] == {
        "pipeline_id": "${resources.pipelines.opensky_silver.id}",
        "full_refresh": False,
    }

    validation_task = tasks["validate_opensky_silver"]
    assert validation_task["depends_on"] == [
        {"task_key": "refresh_opensky_silver"}
    ]


def test_pipeline_sql_preserves_agreed_quality_policy() -> None:
    sql = PIPELINE_SQL_PATH.read_text(encoding="utf-8")

    assert "FROM STREAM(bronze.raw_opensky_states)" in sql
    assert "CREATE OR REFRESH STREAMING TABLE opensky_state" in sql
    assert "CREATE OR REFRESH STREAMING TABLE opensky_state_quarantine" in sql
    assert "velocity_mps IS NULL OR velocity_mps >= 0.0" in sql
    assert "POSITION_TIMESTAMP_FROM_FUTURE" in sql
    assert "INVALID_POSITION_TIME" not in sql
    assert "position_ts < snapshot_ts" not in sql
    assert "vertical_rate_mps >=" not in sql
    assert "baro_altitude_m <=" not in sql
    assert "geo_altitude_m <=" not in sql
