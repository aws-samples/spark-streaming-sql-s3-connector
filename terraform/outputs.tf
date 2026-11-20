# e2e_tests reads these with `terraform output -json`.

output "region" {
  value = var.region
}

output "bucket" {
  value = aws_s3_bucket.e2e.bucket
}

output "job_role_arn" {
  value = aws_iam_role.job.arn
}

output "applications" {
  description = "Release-label tag -> { id, release_label }."
  value = {
    for k, app in aws_emrserverless_application.spark : k => {
      id            = app.id
      release_label = app.release_label
    }
  }
}

output "queues" {
  description = "Queue name (scenario or fan-out) -> { queue_url, dlq_url, prefix }."
  value = merge(
    {
      for s in var.scenarios : s => {
        queue_url = aws_sqs_queue.scenario[s].url
        dlq_url   = aws_sqs_queue.scenario_dlq[s].url
        prefix    = "input/${s}/"
      }
    },
    {
      for q in local.fanout_queues : q => {
        queue_url = aws_sqs_queue.fanout[q].url
        dlq_url   = aws_sqs_queue.fanout_dlq[q].url
        prefix    = "input/fanout/"
      }
    },
  )
}

output "max_receive_count" {
  value = var.max_receive_count
}

output "remote" {
  description = "Remote source, or null when var.remote.enabled is false."
  value = var.remote.enabled ? {
    region          = module.remote[0].region
    bucket          = module.remote[0].bucket
    queue_url       = module.remote[0].queue_url
    dlq_url         = module.remote[0].dlq_url
    prefix          = "input/remote/"
    assume_role_arn = var.remote.assume_role_arn
  } : null
}
