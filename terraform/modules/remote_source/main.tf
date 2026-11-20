# A source bucket and queue in another region and/or account. The provider passed in decides
# where. The job role lives in the main account; the policies below let it read and consume.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
  }
}

variable "name" {
  type = string
}

variable "job_role_arn" {
  description = "EMR Serverless job role in the main account."
  type        = string
}

variable "max_receive_count" {
  type = number
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region
}

resource "aws_s3_bucket" "source" {
  bucket        = "${var.name}-${local.account_id}-${local.region}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "source" {
  bucket = aws_s3_bucket.source.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "source" {
  bucket = aws_s3_bucket.source.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "source" {
  bucket = aws_s3_bucket.source.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "source" {
  bucket = aws_s3_bucket.source.id

  rule {
    id     = "expire-test-data"
    status = "Enabled"

    filter {}

    expiration {
      days = 3
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

data "aws_iam_policy_document" "bucket" {
  # Deny any request that is not over TLS.
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.source.arn, "${aws_s3_bucket.source.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # The job role may list the bucket and look up its region (EMRFS). Nothing else at bucket level.
  statement {
    sid       = "JobRoleListBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.source.arn]

    principals {
      type        = "AWS"
      identifiers = [var.job_role_arn]
    }

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["input", "input/*"]
    }
  }

  statement {
    sid       = "JobRoleBucketLocation"
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.source.arn]

    principals {
      type        = "AWS"
      identifiers = [var.job_role_arn]
    }
  }

  # The job role may read fixture files under input/ only. Read-only: the job never writes here.
  statement {
    sid       = "JobRoleReadInput"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.source.arn}/input/*"]

    principals {
      type        = "AWS"
      identifiers = [var.job_role_arn]
    }
  }
}

resource "aws_s3_bucket_policy" "source" {
  bucket = aws_s3_bucket.source.id
  policy = data.aws_iam_policy_document.bucket.json

  depends_on = [aws_s3_bucket_public_access_block.source]
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = 86400
}

resource "aws_sqs_queue" "source" {
  name                       = var.name
  sqs_managed_sse_enabled    = true
  message_retention_seconds  = 14400
  visibility_timeout_seconds = 60

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

data "aws_iam_policy_document" "queue" {
  # S3 may send events of this bucket, in this account, to the queue.
  statement {
    sid       = "AllowS3BucketEvents"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.source.arn]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.source.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }

  # The job role may consume: receive, delete (single and batch) and change visibility.
  statement {
    sid       = "JobRoleConsume"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"]
    resources = [aws_sqs_queue.source.arn]

    principals {
      type        = "AWS"
      identifiers = [var.job_role_arn]
    }
  }

  # Deny any request that is not over TLS.
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.source.arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_sqs_queue_policy" "source" {
  queue_url = aws_sqs_queue.source.id
  policy    = data.aws_iam_policy_document.queue.json
}

resource "aws_s3_bucket_notification" "source" {
  bucket = aws_s3_bucket.source.id

  queue {
    queue_arn     = aws_sqs_queue.source.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "input/remote/"
  }

  depends_on = [aws_sqs_queue_policy.source]
}

output "region" {
  value = local.region
}

output "bucket" {
  value = aws_s3_bucket.source.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.source.arn
}

output "queue_url" {
  value = aws_sqs_queue.source.url
}

output "queue_arn" {
  value = aws_sqs_queue.source.arn
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.url
}
