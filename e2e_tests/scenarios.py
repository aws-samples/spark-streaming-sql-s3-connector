"""Scenario names. Each gets its own prefix input/<name>/, queue and DLQ in Terraform.

run_e2e.py passes the list of the chosen suite to `terraform apply -var scenarios=...`, so this
file is the single source of truth. The fan-out queues (fanout_a, fanout_b) are fixed in terraform/sns.tf.
"""

CORE_SCENARIOS = [
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

# test_pyspark.py (marker "pyspark"): the same use cases through the PySpark helper package.
PYSPARK_SCENARIOS = [
    "py_fmt_csv",
    "py_fmt_json",
    "py_fmt_parquet",
    "py_fmt_orc",
    "py_fmt_text",
    "py_partitions",
    "py_options",
    "py_option_errors",
    "py_stateful_hdfs",
]

# run_e2e.py --suite: which scenarios to create and which tests to run (pytest -m).
SUITES = {
    "all": (CORE_SCENARIOS + PYSPARK_SCENARIOS, None),
    "core": (CORE_SCENARIOS, "not pyspark"),
    "pyspark": (PYSPARK_SCENARIOS, "pyspark"),
}

SCENARIOS = SUITES["all"][0]
