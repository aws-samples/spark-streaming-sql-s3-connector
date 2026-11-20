# One EMR Serverless application per release label, so one run covers several Spark versions.
# Applications cost nothing while stopped; they start on the first job and stop 5 minutes idle.
resource "aws_emrserverless_application" "spark" {
  for_each = var.release_labels

  name          = "${local.name}-${each.key}"
  release_label = each.value
  type          = "spark"

  maximum_capacity {
    cpu    = var.max_capacity.cpu
    memory = var.max_capacity.memory
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }

  network_configuration {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.emr.id]
  }
}
