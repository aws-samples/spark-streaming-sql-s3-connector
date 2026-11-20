# Runtime role for the EMR Serverless test jobs. The test harness passes it to StartJobRun.

data "aws_iam_policy_document" "job_trust" {
  # Only EMR Serverless applications of this stack, in this account, may assume the role.
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      # The application ARN, or a job-run ARN under it, depending on what the service sends.
      values = flatten([for app in aws_emrserverless_application.spark : [app.arn, "${app.arn}/*"]])
    }
  }
}

resource "aws_iam_role" "job" {
  name                 = "${local.name}-job"
  description          = "spark-streaming-sql-s3-connector e2e job runtime role"
  assume_role_policy   = data.aws_iam_policy_document.job_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "job" {
  # The connector lists input prefixes, Spark lists checkpoint and output folders,
  # and EMRFS asks for the bucket's region. Bucket-level actions, this bucket only.
  statement {
    sid       = "BucketLocation"
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.e2e.arn]
  }

  # ListBucket only for the prefixes the job uses, as the README policy recommends (s3:prefix condition)
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.e2e.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["input", "input/*", "run", "run/*", "logs", "logs/*", "artifacts", "artifacts/*"]
    }
  }

  # Read the jar and job script (artifacts/), the fixtures (input/), and the job's own
  # checkpoint and output on restart (run/). The bare "run", "input" and "logs" keys are
  # there because Hadoop mkdirs/exists probes each parent "directory" with a HEAD; without
  # GetObject that HEAD is a 403 instead of a 404.
  statement {
    sid     = "ReadObjects"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.e2e.arn}/artifacts/*",
      "${aws_s3_bucket.e2e.arn}/input",
      "${aws_s3_bucket.e2e.arn}/input/*",
      "${aws_s3_bucket.e2e.arn}/run",
      "${aws_s3_bucket.e2e.arn}/run/*",
      "${aws_s3_bucket.e2e.arn}/logs",
    ]
  }

  # Write checkpoints, RocksDB metadata log, sink output and status files (run/), and job
  # logs (logs/). Delete is needed because Spark's checkpoint commits rename (copy + delete)
  # and RocksDB maintenance removes old versions. The job never writes input/ or artifacts/.
  statement {
    sid     = "WriteRunAndLogs"
    actions = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.e2e.arn}/run/*",
      "${aws_s3_bucket.e2e.arn}/logs/*",
    ]
  }

  # The connector's SQS client: receiveMessage, deleteMessage / deleteMessageBatch
  # (both need sqs:DeleteMessage) and changeMessageVisibility. Scenario and fan-out
  # queues only, not the DLQs; the job never sends messages.
  statement {
    sid       = "ConsumeQueues"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"]
    resources = [for q in local.all_queues : q.arn]
  }

  # Same read and consume rights on the optional remote bucket and queue. The remote
  # account's bucket and queue policies must also allow this role (see modules/remote_source).
  dynamic "statement" {
    for_each = var.remote.enabled ? [1] : []
    content {
      sid       = "RemoteListBucket"
      actions   = ["s3:ListBucket"]
      resources = [module.remote[0].bucket_arn]
      condition {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = ["input", "input/*"]
      }
    }
  }

  dynamic "statement" {
    for_each = var.remote.enabled ? [1] : []
    content {
      sid       = "RemoteBucketLocation"
      actions   = ["s3:GetBucketLocation"]
      resources = [module.remote[0].bucket_arn]
    }
  }

  dynamic "statement" {
    for_each = var.remote.enabled ? [1] : []
    content {
      sid       = "RemoteReadInput"
      actions   = ["s3:GetObject"]
      resources = ["${module.remote[0].bucket_arn}/input/*"]
    }
  }

  dynamic "statement" {
    for_each = var.remote.enabled ? [1] : []
    content {
      sid       = "RemoteConsumeQueue"
      actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"]
      resources = [module.remote[0].queue_arn]
    }
  }
}

resource "aws_iam_role_policy" "job" {
  name   = "e2e-job"
  role   = aws_iam_role.job.id
  policy = data.aws_iam_policy_document.job.json
}
