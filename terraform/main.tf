data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  name       = var.name_prefix

  tags = merge(var.tags, {
    Project   = "spark-streaming-sql-s3-connector-e2e"
    ManagedBy = "terraform"
  })

  # Two fan-out queues subscribed to one SNS topic; the topic gets the S3 events of input/fanout/.
  fanout_queues = toset(["fanout_a", "fanout_b"])

  # All queues the job reads from: direct S3 -> SQS scenarios plus the fan-out queues.
  all_queues = merge(aws_sqs_queue.scenario, aws_sqs_queue.fanout)
}
