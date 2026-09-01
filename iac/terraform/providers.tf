terraform {
  required_version = ">= 1.6.0"

  # Configure HCP Terraform without publishing an organization or workspace name.
  # Set TF_CLOUD_ORGANIZATION and TF_WORKSPACE before terraform init.
  cloud {}

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.129.0"
    }
  }
}

# Authentication is supplied by the Databricks CLI profile or environment.
# For local Free Edition work, set DATABRICKS_CONFIG_PROFILE=databricks-free.
provider "databricks" {}
