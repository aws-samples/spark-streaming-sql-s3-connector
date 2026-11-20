"""Unit tests for the options builder and read_stream. No Spark needed."""

import pytest

import spark_s3_connector as c

REQUIRED = dict(
    file_format="csv",
    queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/q",
    queue_region="us-east-1",
)


def test_required_only():
    assert c.s3_connector_options(**REQUIRED) == {
        "spark.s3conn.fileFormat": "csv",
        "spark.s3conn.queueUrl": REQUIRED["queue_url"],
        "spark.s3conn.queueRegion": "us-east-1",
        "spark.s3conn.queueType": "SQS",
    }


def test_all_options_and_one_dot_sqs_keys():
    opts = c.s3_connector_options(
        **REQUIRED,
        max_files_per_trigger=500,
        max_file_age="12h",
        path_glob_filter="*.csv",
        partition_columns=["a", "b"],
        base_path="s3://bucket/root/",
        queue_fetch_wait_timeout_seconds=10,
        reprocess_start_batch_id=0,
        reprocess_end_batch_id=3,
        reprocess_dry_run=False,
        sqs_long_polling_wait_time_seconds=0,
        sqs_max_concurrency=5,
        sqs_max_retries=7,
        sqs_visibility_timeout_seconds=90,
        sqs_keep_message_for_consumer_error=True,
        extra_options={"header": "true"},
    )
    assert opts == {
        "spark.s3conn.fileFormat": "csv",
        "spark.s3conn.queueUrl": REQUIRED["queue_url"],
        "spark.s3conn.queueRegion": "us-east-1",
        "spark.s3conn.queueType": "SQS",
        "spark.s3conn.maxFilesPerTrigger": "500",
        "spark.s3conn.maxFileAge": "12h",
        "spark.s3conn.pathGlobFilter": "*.csv",
        "spark.s3conn.partitionColumns": "a,b",
        "basePath": "s3://bucket/root/",
        "spark.s3conn.queueFetchWaitTimeoutSeconds": "10",
        "spark.s3conn.reprocessStartBatchId": "0",
        "spark.s3conn.reprocessEndBatchId": "3",
        "spark.s3conn.reprocessDryRun": "false",
        "spark.s3conn.sqs.longPollingWaitTimeSeconds": "0",
        "spark.s3conn.sqs.maxConcurrency": "5",
        "spark.s3conn.sqs.maxRetries": "7",
        "spark.s3conn.sqs.visibilityTimeoutSeconds": "90",
        "spark.s3conn.sqs.keepMessageForConsumerError": "true",
        "header": "true",
    }
    assert not any("..sqs." in k for k in opts)


def test_unlimited_files_and_partition_string():
    opts = c.s3_connector_options(**REQUIRED, max_files_per_trigger=-1, partition_columns=" a , b ")
    assert opts["spark.s3conn.maxFilesPerTrigger"] == "-1"
    assert opts["spark.s3conn.partitionColumns"] == "a,b"


@pytest.mark.parametrize("age", ["15d", "12h", "30m", "5min", "90s", "500ms", "10us", "1000", " 2D "])
def test_valid_ages(age):
    assert c.s3_connector_options(**REQUIRED, max_file_age=age)["spark.s3conn.maxFileAge"] == age.strip()


@pytest.mark.parametrize(
    "kwargs, error",
    [
        (dict(file_format=""), ValueError),
        (dict(queue_url=None), ValueError),
        (dict(queue_region="  "), ValueError),
        (dict(queue_type="KAFKA"), ValueError),
        (dict(max_files_per_trigger=0), ValueError),
        (dict(max_files_per_trigger=-2), ValueError),
        (dict(max_files_per_trigger="10"), TypeError),
        (dict(max_files_per_trigger=True), TypeError),
        (dict(max_file_age="15 days"), ValueError),
        (dict(max_file_age="-1d"), ValueError),
        (dict(max_file_age=15), ValueError),
        (dict(partition_columns="a,,b"), ValueError),
        (dict(queue_fetch_wait_timeout_seconds=0), ValueError),
        (dict(reprocess_start_batch_id=1), ValueError),
        (dict(reprocess_end_batch_id=1), ValueError),
        (dict(reprocess_start_batch_id=-1, reprocess_end_batch_id=1), ValueError),
        (dict(reprocess_start_batch_id=3, reprocess_end_batch_id=1), ValueError),
        (dict(reprocess_dry_run="false"), TypeError),
        (dict(sqs_long_polling_wait_time_seconds=21), ValueError),
        (dict(sqs_long_polling_wait_time_seconds=-1), ValueError),
        (dict(sqs_max_concurrency=0), ValueError),
        (dict(sqs_max_retries=1.5), TypeError),
        (dict(sqs_visibility_timeout_seconds=0), ValueError),
        (dict(sqs_keep_message_for_consumer_error=1), TypeError),
        (dict(extra_options={"spark.s3conn.fileFormat": "json"}), ValueError),
        (dict(extra_options={"k": 1}), TypeError),
        (dict(extra_options={"SPARK.S3CONN.FILEFORMAT": "json"}), ValueError),
        (dict(sqs_max_retries=2**31), ValueError),
        (dict(max_files_per_trigger=2**31), ValueError),
        (dict(max_file_age="99999999999999999999d"), ValueError),
    ],
)
def test_rejected(kwargs, error):
    with pytest.raises(error):
        c.s3_connector_options(**{**REQUIRED, **kwargs})


def test_strings_are_stripped_and_int_max_allowed():
    opts = c.s3_connector_options(
        file_format=" csv ", queue_url=REQUIRED["queue_url"], queue_region="us-east-1 ",
        sqs_max_retries=2**31 - 1,
    )
    assert opts["spark.s3conn.fileFormat"] == "csv"
    assert opts["spark.s3conn.queueRegion"] == "us-east-1"
    assert opts["spark.s3conn.sqs.maxRetries"] == str(2**31 - 1)


def test_required_are_required():
    with pytest.raises(TypeError):
        c.s3_connector_options(file_format="csv", queue_url="u")  # no queue_region


@pytest.mark.parametrize(
    "version, scala",
    [("3.2.1", "2.12"), ("3.3.0", "2.12"), ("3.5.8-amzn-0", "2.12"), ("4.1.1", "2.13")],
)
def test_scala_binary_version(version, scala):
    assert c.scala_binary_version(version) == scala


@pytest.mark.parametrize("version", ["2.4.8", "5.0.0", "x"])
def test_scala_binary_version_rejects(version):
    with pytest.raises(ValueError):
        c.scala_binary_version(version)


class _Reader:
    """Stands in for DataStreamReader and records the calls."""

    def __init__(self, error=None):
        self.calls, self.error = [], error

    def format(self, f):
        self.calls.append(("format", f))
        return self

    def schema(self, s):
        self.calls.append(("schema", s))
        return self

    def options(self, **o):
        self.calls.append(("options", o))
        return self

    def load(self):
        if self.error:
            raise self.error
        return "df"


class _Spark:
    version = "3.5.8"

    def __init__(self, reader):
        self.readStream = reader


def test_read_stream_calls_reader():
    reader = _Reader()
    assert c.read_stream(_Spark(reader), "a STRING", **REQUIRED) == "df"
    assert reader.calls == [
        ("format", "s3-connector"),
        ("schema", "a STRING"),
        ("options", c.s3_connector_options(**REQUIRED)),
    ]


def test_read_stream_needs_schema():
    with pytest.raises(ValueError, match="schema"):
        c.read_stream(_Spark(_Reader()), None, **REQUIRED)


@pytest.mark.parametrize(
    "message",
    [
        "Failed to find data source: s3-connector. Please find packages at ...",
        "[DATA_SOURCE_NOT_FOUND] Failed to find the data source: s3-connector.",
    ],
)
def test_read_stream_missing_jar(message):
    with pytest.raises(c.ConnectorNotFoundError, match="Scala 2.12"):
        c.read_stream(_Spark(_Reader(RuntimeError(message))), "a STRING", **REQUIRED)


def test_read_stream_missing_jar_unknown_spark_keeps_hint():
    spark = _Spark(_Reader(RuntimeError("Failed to find data source: s3-connector")))
    spark.version = "5.0.0"
    with pytest.raises(c.ConnectorNotFoundError, match="built for Spark 5.0.0 with"):
        c.read_stream(spark, "a STRING", **REQUIRED)


def test_read_stream_other_errors_pass_through():
    with pytest.raises(KeyError):
        c.read_stream(_Spark(_Reader(KeyError("x"))), "a STRING", **REQUIRED)
