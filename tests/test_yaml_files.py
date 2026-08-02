from pathlib import Path

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
YAML_FILES = sorted(
    [
        REPOSITORY_ROOT / "databricks.yml",
        *REPOSITORY_ROOT.glob("configs/**/*.yml"),
        *REPOSITORY_ROOT.glob("resources/**/*.yml"),
        *REPOSITORY_ROOT.glob(".github/workflows/*.yml"),
    ]
)


@pytest.mark.parametrize(
    "yaml_path",
    YAML_FILES,
    ids=lambda path: str(path.relative_to(REPOSITORY_ROOT)),
)
def test_yaml_file_parses_to_mapping(yaml_path: Path) -> None:
    content = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))

    assert isinstance(content, dict)
    assert content


def test_public_bundle_defaults_to_paused_schedule() -> None:
    bundle = yaml.safe_load((REPOSITORY_ROOT / "databricks.yml").read_text(encoding="utf-8"))
    job_definition = (
        REPOSITORY_ROOT / "resources" / "jobs" / "bronze_ingestion_e2e.job.yml"
    ).read_text(encoding="utf-8")

    assert bundle["variables"]["job_pause_status"]["default"] == "PAUSED"
    assert bundle["targets"]["dev"]["variables"]["job_pause_status"] == "PAUSED"
    assert "pause_status: ${var.job_pause_status}" in job_definition
