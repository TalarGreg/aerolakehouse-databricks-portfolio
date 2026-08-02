variable "catalog_name" {
  description = "Existing Unity Catalog catalog bootstrapped with Free Edition Default Storage"
  type        = string
  default     = "dev_alh"

  validation {
    condition     = can(regex("^[A-Za-z_][A-Za-z0-9_]*$", var.catalog_name))
    error_message = "catalog_name must be a valid Unity Catalog identifier."
  }
}

variable "schema_names" {
  description = "Schemas managed inside the development catalog"
  type        = set(string)
  default     = ["bronze", "silver", "gold", "monitoring"]

  validation {
    condition = alltrue([
      for name in var.schema_names : can(regex("^[A-Za-z_][A-Za-z0-9_]*$", name))
    ])
    error_message = "Every schema name must be a valid Unity Catalog identifier."
  }
}

variable "raw_volume_name" {
  description = "Managed Unity Catalog volume used for raw ingestion files"
  type        = string
  default     = "raw_files"

  validation {
    condition     = can(regex("^[A-Za-z_][A-Za-z0-9_]*$", var.raw_volume_name))
    error_message = "raw_volume_name must be a valid Unity Catalog identifier."
  }
}

variable "secret_scope_name" {
  description = "Databricks secret scope used by development ingestion notebooks"
  type        = string
  default     = "aerolakehouse-dev"

  validation {
    condition     = can(regex("^[A-Za-z0-9._-]+$", var.secret_scope_name))
    error_message = "secret_scope_name contains unsupported characters."
  }
}
