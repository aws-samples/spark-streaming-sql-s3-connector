# One private bucket holds everything for a run:
#   artifacts/  connector jar and job script (uploaded by the test harness)
#   input/<scenario>/  fixture files; ObjectCreated events go to that scenario's queue
#   run/<scenario>/    checkpoints and job output
#   logs/       EMR Serverless driver and executor logs
resource "aws_s3_bucket" "e2e" {
  bucket = "${local.name}-${local.account_id}-${var.region}"

  # Test data only; lets `terraform destroy` remove a non-empty bucket.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "e2e" {
  bucket = aws_s3_bucket.e2e.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "e2e" {
  bucket = aws_s3_bucket.e2e.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# SSE-S3: encrypted at rest without a KMS key the S3 event and EMR paths would need grants on.
resource "aws_s3_bucket_server_side_encryption_configuration" "e2e" {
  bucket = aws_s3_bucket.e2e.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Safety net if a run is never destroyed: objects expire after 3 days.
resource "aws_s3_bucket_lifecycle_configuration" "e2e" {
  bucket = aws_s3_bucket.e2e.id

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
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.e2e.arn,
      "${aws_s3_bucket.e2e.arn}/*",
    ]

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

resource "aws_s3_bucket_policy" "e2e" {
  bucket = aws_s3_bucket.e2e.id
  policy = data.aws_iam_policy_document.bucket.json

  depends_on = [aws_s3_bucket_public_access_block.e2e]
}

# One notification config per bucket: each scenario prefix -> its queue, input/fanout/ -> SNS.
resource "aws_s3_bucket_notification" "e2e" {
  bucket = aws_s3_bucket.e2e.id

  dynamic "queue" {
    for_each = aws_sqs_queue.scenario
    content {
      id            = queue.key
      queue_arn     = queue.value.arn
      events        = ["s3:ObjectCreated:*"]
      filter_prefix = "input/${queue.key}/"
    }
  }

  topic {
    id            = "fanout"
    topic_arn     = aws_sns_topic.fanout.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "input/fanout/"
  }

  # S3 checks it may publish when the notification is saved.
  depends_on = [
    aws_sqs_queue_policy.scenario,
    aws_sns_topic_policy.fanout,
  ]
}
