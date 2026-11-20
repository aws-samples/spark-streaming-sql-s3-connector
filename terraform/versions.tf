terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}

# Account and region of the optional remote source (cross-region and/or cross-account).
# When var.remote.enabled is false nothing is created with this provider.
provider "aws" {
  alias  = "remote"
  region = coalesce(var.remote.region, var.region)

  dynamic "assume_role" {
    for_each = var.remote.assume_role_arn == null ? [] : [var.remote.assume_role_arn]
    content {
      role_arn     = assume_role.value
      session_name = "spark-s3-connector-e2e"
    }
  }

  default_tags {
    tags = local.tags
  }
}
