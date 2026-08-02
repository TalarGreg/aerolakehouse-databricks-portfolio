import copy
import re
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIRECTORY = REPOSITORY_ROOT / ".github" / "workflows"
COMMIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class GitHubWorkflowLoader(yaml.SafeLoader):
    pass


GitHubWorkflowLoader.yaml_implicit_resolvers = copy.deepcopy(
    yaml.SafeLoader.yaml_implicit_resolvers
)
for resolver_key, resolvers in GitHubWorkflowLoader.yaml_implicit_resolvers.items():
    GitHubWorkflowLoader.yaml_implicit_resolvers[resolver_key] = [
        resolver for resolver in resolvers if resolver[0] != "tag:yaml.org,2002:bool"
    ]


def _load_workflow(name: str) -> dict:
    workflow_path = WORKFLOW_DIRECTORY / name
    return yaml.load(
        workflow_path.read_text(encoding="utf-8"),
        Loader=GitHubWorkflowLoader,
    )


def _external_action_references(workflow: dict) -> list[str]:
    references = []
    for job in workflow["jobs"].values():
        if "uses" in job and not job["uses"].startswith("./"):
            references.append(job["uses"])
        for step in job.get("steps", []):
            if "uses" in step and not step["uses"].startswith("./"):
                references.append(step["uses"])
    return references


def test_ci_can_be_used_as_credential_free_preflight() -> None:
    workflow = _load_workflow("ci.yml")

    assert "workflow_call" in workflow["on"]
    assert workflow["permissions"] == {"contents": "read"}
    assert "secrets." not in (WORKFLOW_DIRECTORY / "ci.yml").read_text(encoding="utf-8")


def test_deployment_is_manual_main_only_and_serialized() -> None:
    workflow = _load_workflow("deploy-dev.yml")
    dispatch = workflow["on"]["workflow_dispatch"]

    assert set(workflow["on"]) == {"workflow_dispatch", "workflow_run"}
    assert dispatch["inputs"]["operation"]["default"] == "plan"
    assert dispatch["inputs"]["operation"]["options"] == ["plan", "deploy"]
    assert workflow["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
    }
    assert workflow["concurrency"] == {
        "group": "deploy-development",
        "cancel-in-progress": "false",
    }
    assert workflow["jobs"]["validate"]["uses"] == "./.github/workflows/ci.yml"
    assert "workflow_dispatch" in workflow["jobs"]["validate"]["if"]
    assert workflow["jobs"]["deploy"]["environment"] == {"name": "dev"}

    workflow_text = (WORKFLOW_DIRECTORY / "deploy-dev.yml").read_text(encoding="utf-8")
    assert 'GITHUB_REF}" != "refs/heads/main"' in workflow_text
    assert "terraform apply -input=false -auto-approve tfplan" in workflow_text
    assert "databricks bundle deploy --target dev" in workflow_text
    assert "databricks bundle run" not in workflow_text


def test_automatic_deployment_uses_successful_main_ci_commit() -> None:
    workflow = _load_workflow("deploy-dev.yml")
    workflow_run = workflow["on"]["workflow_run"]
    prepare_job = workflow["jobs"]["prepare"]
    deploy_job = workflow["jobs"]["deploy"]

    assert workflow_run == {
        "workflows": ["CI"],
        "types": ["completed"],
        "branches": ["main"],
    }
    assert set(prepare_job["outputs"]) == {
        "deploy_required",
        "operation",
        "target_sha",
    }
    assert "secrets." not in str(prepare_job)
    assert set(deploy_job["needs"]) == {"validate", "prepare"}
    assert "needs.prepare.outputs.deploy_required == 'true'" in deploy_job["if"]
    assert "github.event_name == 'workflow_run'" in deploy_job["if"]

    workflow_text = (WORKFLOW_DIRECTORY / "deploy-dev.yml").read_text(encoding="utf-8")
    assert "github.event.workflow_run.head_sha" in workflow_text
    assert "ENABLE_AUTOMATIC_DEPLOYMENT" in workflow_text
    assert "Automatic deployment is disabled for this repository." in workflow_text
    assert 'UPSTREAM_EVENT}" != "push"' in workflow_text
    assert 'UPSTREAM_BRANCH}" != "main"' in workflow_text
    assert 'UPSTREAM_CONCLUSION}" != "success"' in workflow_text
    assert "commits/${TARGET_SHA}/pulls" in workflow_text
    assert ".previous_filename // empty" in workflow_text
    assert "No associated PR found; deploying direct push conservatively." in workflow_text
    assert "No Databricks or Terraform deployment paths changed." not in workflow_text


def test_external_actions_are_pinned_to_commit_shas() -> None:
    for workflow_name in ("ci.yml", "deploy-dev.yml"):
        workflow = _load_workflow(workflow_name)

        for reference in _external_action_references(workflow):
            _, version = reference.rsplit("@", 1)
            assert COMMIT_SHA_PATTERN.fullmatch(version), (
                f"{workflow_name}: {reference} must use a full commit SHA"
            )
