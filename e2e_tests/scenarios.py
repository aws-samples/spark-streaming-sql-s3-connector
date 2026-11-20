"""Scenario names. Each gets its own prefix input/<name>/, queue and DLQ in Terraform.

run_e2e.py passes this list to `terraform apply -var scenarios=...`, so this file is the
single source of truth. The fan-out queues (fanout_a, fanout_b) are fixed in terraform/sns.tf.
"""

SCENARIOS = [
    "fmt_csv",
    "fmt_json",
    "fmt_parquet",
    "fmt_orc",
    "fmt_text",
    "csv_header",
    "malformed",
    "partitions",
    "mfpt_limit",
    "mfpt_unlimited",
    "glob",
    "file_age",
    "duplicates",
    "bad_messages",
    "keep_double_dot",
    "keep_single_dot",
    "reprocess",
    "recovery",
    "stateful_hdfs",
    "stateful_rocksdb",
    "failures",
    "reprocess_partial",
    "wrong_region",
    "special_keys",
    "space_key",
]
