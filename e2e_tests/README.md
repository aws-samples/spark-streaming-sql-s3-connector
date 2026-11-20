# End-to-end tests

Real Spark jobs (EMR Serverless) read real S3 files through real SQS events with the connector jar.
The use cases and the test for each are in `../USE-CASES.md`.

## Run everything (creates and then destroys AWS resources)

```bash
cd spark-streaming-sql-s3-connector
mvn -Pdefault-test,spark-3.5 -Dspark.version=3.5.2 clean package -DskipTests   # JDK 11
python3 -m venv e2e_tests/.venv && e2e_tests/.venv/bin/pip install -r e2e_tests/requirements.txt
e2e_tests/.venv/bin/python e2e_tests/run_e2e.py \
  --connector-jar target/spark-streaming-sql-s3-connector-0.0.3-spark3.5.2.jar \
  --release-label spark35=emr-7.5.0
```

`run_e2e.py` runs `terraform apply`, then pytest once per `--release-label`, then always stops the
EMR applications and runs `terraform destroy`. Exit code is the worst pytest exit code.
Use temporary credentials of an IAM role for the target account in the shell (default profile or
`AWS_PROFILE`), not long-term access keys.
Run one at a time per account and region with the same `name_prefix` (resource names are fixed; set
`name_prefix` in a `terraform/*.auto.tfvars.json` file to run two stacks side by side).
The core suite takes about 1.5 to 2 hours per release label; the NAT gateway, Elastic IP and EMR
Serverless jobs are billed while the stack exists.

- More Spark versions: repeat `--release-label TAG=LABEL`, one jar per tag with
  `--connector-jar TAG=PATH`, built for that release's Spark version, e.g.
  `--release-label spark41=emr-spark-8.2.0 --connector-jar spark41=target/spark-streaming-sql-s3-connector-0.0.3-spark4.1.3.jar`
  (built with `-Pdefault-test,spark-4.1 -Dspark.version=4.1.3` on JDK 17).
- Suites: `--suite all` (default), `--suite core` (raw option keys, as before the PySpark lane) or
  `--suite pyspark` (15 cases through the `python/` helper package, `job/pyspark_job.py` with the
  package on `--py-files`; about 30 to 40 minutes per release label). Only the chosen suite's
  queues are created.
- Cross-region: `--remote-region us-west-2`. Cross-account: add
  `--remote-assume-role-arn arn:aws:iam::<B>:role/<role>`. That role is used by Terraform
  (create a bucket, queue, policies and notification in account B) and by the harness
  (`s3:PutObject` on `input/remote/*`, `sqs:PurgeQueue` and `sqs:GetQueueAttributes` on the queues).
- Pass pytest options after `--`, e.g. `-- -k reprocess` or `-- -m "not sqs_single_dot"`.

## Offline checks

```bash
cd spark-streaming-sql-s3-connector/e2e_tests
.venv/bin/python -m pytest unit -q          # pure helpers
.venv/bin/python -m pytest --collect-only -q
cd ../terraform && terraform init -backend=false && terraform validate && tflint --recursive
```

## Rights the person running the tests needs

Terraform needs create/delete rights for S3, SQS, SNS, KMS, IAM (one role + inline policy),
EC2/VPC (VPC, subnets, NAT, EIP, endpoint, security groups) and EMR Serverless.
The harness needs, on the stack's resources: `s3:PutObject/GetObject/ListBucket/DeleteObject`,
`sqs:SendMessage/ReceiveMessage/DeleteMessage/GetQueueAttributes`,
`emr-serverless:StartJobRun/GetJobRun/CancelJobRun/ListJobRuns/GetApplication/StopApplication`
and `iam:PassRole` on the job role.
