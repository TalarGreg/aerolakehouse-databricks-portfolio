output "current_user" {
  description = "Databricks identity that owns the Free Edition resources"
  value       = data.databricks_current_user.me.user_name
}

output "catalog" {
  description = "Existing Unity Catalog catalog used by Terraform-managed resources"
  value       = data.databricks_catalog.dev.name
}

output "schemas" {
  description = "Unity Catalog schemas managed by Terraform"
  value = sort([
    for schema in databricks_schema.schemas :
    "${schema.catalog_name}.${schema.name}"
  ])
}

output "raw_volume" {
  description = "Managed volume used by ingestion notebooks"
  value       = "${databricks_volume.raw_files.catalog_name}.${databricks_volume.raw_files.schema_name}.${databricks_volume.raw_files.name}"
}

output "secret_scope" {
  description = "Secret scope that must be populated after terraform apply"
  value       = databricks_secret_scope.dev.name
}

output "next_steps" {
  description = "Manual steps that intentionally remain outside Terraform"
  value = [
    "Add openweathermap-api-key to ${databricks_secret_scope.dev.name}",
    "Add aviationstack-api-key to ${databricks_secret_scope.dev.name}",
    "Run the 010 and 030 bootstrap scripts from sql/ddl",
    "Validate and deploy the Databricks bundle",
  ]
}
