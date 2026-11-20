variable "region" {
  description = "Region for the bucket, the queues and the EMR Serverless applications."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for every resource name. Account id and region are appended to the bucket name."
  type        = string
  default     = "s3conn-e2e"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,20}$", var.name_prefix))
    error_message = "name_prefix must be 3-20 chars of a-z, 0-9 and '-' (it is used in bucket and queue names)."
  }
}

variable "release_labels" {
  description = "EMR Serverless release labels to test, keyed by a short tag. One application per entry. Example: { spark35 = \"emr-7.5.0\", spark40 = \"emr-spark-8.0.0\" } (Spark 4 needs a Scala 2.13 connector jar)."
  type        = map(string)

  validation {
    condition     = length(var.release_labels) > 0
    error_message = "Give at least one release label."
  }
}

variable "scenarios" {
  description = "Test scenario names. Each gets its own S3 prefix input/<name>/, queue and DLQ. run_e2e.py passes the list from e2e_tests/scenarios.py."
  type        = set(string)

  validation {
    condition     = alltrue([for s in var.scenarios : can(regex("^[a-z0-9_]{1,40}$", s))])
    error_message = "Scenario names must be 1-40 chars of a-z, 0-9 and '_'."
  }

  # input/fanout/ and the fanout_a/fanout_b queues belong to the SNS fan-out (sns.tf).
  validation {
    condition     = length(setintersection(var.scenarios, ["fanout", "fanout_a", "fanout_b"])) == 0
    error_message = "fanout, fanout_a and fanout_b are reserved for the SNS fan-out queues."
  }
}

variable "max_receive_count" {
  description = "Receives before SQS moves a message to the DLQ. The keep-message-on-error test waits for this many receives."
  type        = number
  default     = 2
}

variable "max_capacity" {
  description = "Upper bound per EMR Serverless application. Caps spend if a job misbehaves."
  type = object({
    cpu    = string
    memory = string
  })
  default = {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }
}

variable "vpc_cidr" {
  description = "CIDR of the test VPC. EMR Serverless needs a VPC to reach SQS (SQS is not reachable without VPC connectivity)."
  type        = string
  default     = "10.42.0.0/24"
}

variable "remote" {
  description = "Optional remote source: a bucket and queue in another region and/or account. Set assume_role_arn to a role in the other account to test cross-account."
  type = object({
    enabled         = bool
    region          = optional(string)
    assume_role_arn = optional(string)
  })
  default = {
    enabled = false
  }
}

variable "tags" {
  description = "Extra tags on every resource."
  type        = map(string)
  default     = {}
}
