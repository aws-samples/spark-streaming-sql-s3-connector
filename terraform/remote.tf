module "remote" {
  source = "./modules/remote_source"
  count  = var.remote.enabled ? 1 : 0

  providers = {
    aws = aws.remote
  }

  name              = "${local.name}-remote"
  job_role_arn      = aws_iam_role.job.arn
  max_receive_count = var.max_receive_count
}
