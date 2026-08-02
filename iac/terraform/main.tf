data "databricks_current_user" "me" {}

data "databricks_catalog" "dev" {
  name = var.catalog_name
}

resource "databricks_schema" "schemas" {
  for_each = var.schema_names

  catalog_name = data.databricks_catalog.dev.name
  name         = each.value
  owner        = data.databricks_current_user.me.user_name
  comment      = "${title(each.value)} schema for ${var.catalog_name}"
}

resource "databricks_volume" "raw_files" {
  catalog_name = data.databricks_catalog.dev.name
  schema_name  = databricks_schema.schemas["bronze"].name
  name         = var.raw_volume_name
  volume_type  = "MANAGED"
  comment      = "Managed volume for AeroLakehouse raw ingestion files"
}
