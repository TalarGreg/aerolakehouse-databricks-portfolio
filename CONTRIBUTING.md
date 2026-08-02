# Contributing

## Development Workflow

1. Create a focused branch from `main`.
2. Keep configuration, code comments, documentation, and commit messages in English.
3. Do not commit secrets, local state, plans, generated data, or workspace exports.
4. Run the local checks before opening a pull request.
5. Describe operational impact, rollback considerations, and any schema changes.

```powershell
python -m pip install --requirement requirements-dev.txt
python -m compileall -q src notebooks
ruff check src tests
pytest
terraform -chdir=iac/terraform fmt -check -recursive
terraform -chdir=iac/terraform init -backend=false
terraform -chdir=iac/terraform validate -no-color
```

Pull requests should be small enough to review and must not combine unrelated
infrastructure, ingestion, and presentation changes.
