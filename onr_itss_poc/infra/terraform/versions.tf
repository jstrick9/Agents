terraform {
  required_version = ">= 1.6.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.50.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

# Account-level provider (GovCloud DoD example)
provider "databricks" {
  alias      = "accounts"
  host       = var.account_host
  account_id = var.databricks_account_id
  auth_type  = "oauth-m2m"
}

variable "account_host" {
  type        = string
  description = "https://accounts-dod.cloud.databricks.mil or https://accounts.cloud.databricks.us"
  default     = "https://accounts-dod.cloud.databricks.mil"
}

variable "databricks_account_id" {
  type        = string
  description = "Databricks account id (not the AWS account id)"
}

variable "aws_region" {
  type    = string
  default = "us-gov-west-1"
}

variable "uc_bucket" {
  type        = string
  description = "GovCloud S3 bucket for UC managed storage (no CUI in the name)"
}

# Example only — uncomment in a real account.
# resource "databricks_storage_credential" "uc" {
#   provider = databricks.accounts
#   name     = "onr-itss-uc-cred"
#   aws_iam_role { role_arn = var.uc_role_arn }
# }
#
# resource "databricks_external_location" "uc_root" {
#   provider        = databricks.accounts
#   name            = "onr-itss-uc-root"
#   url             = "s3://${var.uc_bucket}/uc/"
#   credential_name = databricks_storage_credential.uc.name
#   read_only       = false
# }
