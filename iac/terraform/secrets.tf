# Terraform manages only the scope. Secret values are entered separately so
# they are not persisted in Terraform state.
resource "databricks_secret_scope" "dev" {
  name = var.secret_scope_name
}
