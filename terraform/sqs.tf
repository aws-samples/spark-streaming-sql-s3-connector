resource "aws_sqs_queue" "scenario_dlq" {
  for_each = var.scenarios

  name                      = "${local.name}-${each.key}-dlq"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = 86400
}

resource "aws_sqs_queue" "scenario" {
  for_each = var.scenarios

  name                    = "${local.name}-${each.key}"
  sqs_managed_sse_enabled = true
  # Longer than any single test; the connector sets its own visibility per message.
  message_retention_seconds  = 14400
  visibility_timeout_seconds = 60

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.scenario_dlq[each.key].arn
    maxReceiveCount     = var.max_receive_count
  })
}

data "aws_iam_policy_document" "scenario_queue" {
  for_each = var.scenarios

  # S3 may send events to this queue, only from our bucket in our account.
  statement {
    sid       = "AllowS3BucketEvents"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.scenario[each.key].arn]

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
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.scenario[each.key].arn]

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

resource "aws_sqs_queue_policy" "scenario" {
  for_each = var.scenarios

  queue_url = aws_sqs_queue.scenario[each.key].id
  policy    = data.aws_iam_policy_document.scenario_queue[each.key].json
}
