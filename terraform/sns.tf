# README use case: S3 -> SNS -> several SQS queues, one per streaming application.
# Raw message delivery keeps the body a plain S3 event, which is what the connector parses.

# S3 cannot publish to a topic encrypted with the AWS managed SNS key, so the topic gets its own key.
resource "aws_kms_key" "fanout" {
  description             = "${local.name} SNS fan-out topic"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  policy                  = data.aws_iam_policy_document.fanout_key.json
}

data "aws_iam_policy_document" "fanout_key" {
  # In a key policy, resources = ["*"] means "this key" only.

  # Standard key policy root statement: lets IAM policies in this account manage the key,
  # so the key never becomes unmanageable.
  statement {
    sid       = "AccountAdmin"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:${local.partition}:iam::${local.account_id}:root"]
    }
  }

  # S3 encrypts the event it publishes to the topic, only for our bucket.
  statement {
    sid       = "AllowS3PublishToTopic"
    actions   = ["kms:GenerateDataKey*", "kms:Decrypt"]
    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.e2e.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_sns_topic" "fanout" {
  name              = "${local.name}-fanout"
  kms_master_key_id = aws_kms_key.fanout.arn
}

data "aws_iam_policy_document" "fanout_topic" {
  # S3 may publish events of our bucket to the topic.
  statement {
    sid       = "AllowS3BucketEvents"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.fanout.arn]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.e2e.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }

  # Deny any request that is not over TLS.
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"
    # SNS rejects the wildcard "sns:*" in a topic policy ("action out of service scope"), so list the actions.
    actions = [
      "sns:Publish", "sns:Subscribe", "sns:Receive", "sns:GetTopicAttributes", "sns:SetTopicAttributes",
      "sns:ListSubscriptionsByTopic", "sns:AddPermission", "sns:RemovePermission", "sns:DeleteTopic",
    ]
    resources = [aws_sns_topic.fanout.arn]

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

resource "aws_sns_topic_policy" "fanout" {
  arn    = aws_sns_topic.fanout.arn
  policy = data.aws_iam_policy_document.fanout_topic.json
}

resource "aws_sqs_queue" "fanout_dlq" {
  for_each = local.fanout_queues

  name                      = "${local.name}-${each.key}-dlq"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = 86400
}

resource "aws_sqs_queue" "fanout" {
  for_each = local.fanout_queues

  name                       = "${local.name}-${each.key}"
  sqs_managed_sse_enabled    = true
  message_retention_seconds  = 14400
  visibility_timeout_seconds = 60

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.fanout_dlq[each.key].arn
    maxReceiveCount     = var.max_receive_count
  })
}

data "aws_iam_policy_document" "fanout_queue" {
  for_each = local.fanout_queues

  # Only our fan-out topic may send to the queue.
  statement {
    sid       = "AllowTopic"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.fanout[each.key].arn]

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sns_topic.fanout.arn]
    }
  }

  # Deny any request that is not over TLS.
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.fanout[each.key].arn]

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

resource "aws_sqs_queue_policy" "fanout" {
  for_each = local.fanout_queues

  queue_url = aws_sqs_queue.fanout[each.key].id
  policy    = data.aws_iam_policy_document.fanout_queue[each.key].json
}

resource "aws_sns_topic_subscription" "fanout" {
  for_each = local.fanout_queues

  topic_arn            = aws_sns_topic.fanout.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.fanout[each.key].arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.fanout]
}
