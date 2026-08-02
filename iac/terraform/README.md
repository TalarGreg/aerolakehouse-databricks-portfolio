# Terraform: Databricks Development Prerequisites

This root module provisions the workspace objects required by the Databricks
Asset Bundle:

- `bronze`, `silver`, `gold`, and `monitoring` schemas;
- a managed Unity Catalog Volume for landing files;
- a Databricks-backed secret scope without secret values.

The authenticated development identity owns the schemas. The module contains
no production resources, account-level identities, secret values, Delta
tables, or committed state.

## Ownership Boundary

| Object | Owner |
|---|---|
| Existing Unity Catalog catalog | One-time platform bootstrap |
| Schemas, managed Volume, and secret scope | Terraform |
| Terraform state and locking | HCP Terraform |
| Bootstrap Delta tables | Numbered scripts under `sql/ddl` |
| Jobs, pipelines, notebooks, and runtime files | Databricks Asset Bundle |
| External API secret values | Databricks Secret Scope |

## HCP Terraform Configuration

`providers.tf` intentionally contains an empty `cloud` block. This keeps the
repository portable and prevents publishing a personal HCP organization or
workspace name. Configure the binding before `terraform init`:

```powershell
$env:TF_CLOUD_ORGANIZATION = "<hcp-organization>"
$env:TF_WORKSPACE = "<hcp-workspace>"
terraform login
terraform init
```

The HCP workspace must use local execution mode when the plan is executed by a
developer machine or GitHub Actions runner. HCP Terraform then stores, versions,
locks, and protects the state while provider operations run in the caller.

For credential-free CI, backend initialization is deliberately disabled:

```powershell
terraform init -backend=false
terraform validate -no-color
```

## Catalog Bootstrap

Databricks Free Edition uses Default Storage. Create the development catalog
once in Catalog Explorer and select **Use default storage**. The module reads
that existing catalog through `data.databricks_catalog.dev` and manages only
objects inside it.

Override the default catalog when needed:

```powershell
$env:TF_VAR_catalog_name = "<catalog>"
```

## Authentication

For local development:

```powershell
databricks auth login --host https://<workspace-host> --profile <profile>
$env:DATABRICKS_CONFIG_PROFILE = "<profile>"
```

The GitHub deployment example expects protected environment secrets
`DATABRICKS_TOKEN` and `TF_API_TOKEN`, plus the non-secret variables documented
in the root README. PAT authentication reflects the current Free Edition
development constraint; workload identity federation is preferred where
supported.

## Plan and Apply

```powershell
terraform fmt -check -recursive
terraform validate
terraform plan -out=tfplan
terraform apply tfplan
```

Review every plan before applying it. Do not apply when remote state is
unavailable, the expected state addresses are missing, the workspace is locked
by another run, or the plan unexpectedly proposes replacements or deletions.

The deployment workflow adds a second guard that rejects any plan containing a
delete action before applying the exact saved plan.

## Runtime Secrets

Secret values are excluded from Terraform because managed values are persisted
in state. After Terraform creates the scope, add API keys through the Databricks
CLI or workspace UI:

```powershell
databricks secrets put-secret <scope> openweathermap-api-key
databricks secrets put-secret <scope> aviationstack-api-key
```

Never commit `.terraform/`, state files, state backups, plans, credentials, or
environment-specific `.tfvars` files. Use HCP Terraform state-version history
for recovery; do not edit state JSON manually.
