"""Local-mode smoke tests against a real PySpark and the connector jar.

Set CONNECTOR_JAR to the connector jar built for this Spark line. Local mode
cannot reach SQS, so no messages flow: the tests check that the source
resolves, that the key names match the Scala code, and that bad options are
rejected by the real Scala parser.
"""

import os
import subprocess
import sys
import textwrap

import pytest

pyspark = pytest.importorskip("pyspark")

import spark_s3_connector as c  # noqa: E402

pytestmark = pytest.mark.spark

JAR = os.environ.get("CONNECTOR_JAR")
needs_jar = pytest.mark.skipif(not JAR, reason="CONNECTOR_JAR is not set")

SCHEMA = "valString STRING, valInt INT, part STRING"
REQUIRED = dict(
    file_format="csv",
    queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/q",
    queue_region="us-east-1",
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    assert JAR and os.path.isfile(JAR), f"CONNECTOR_JAR not found: {JAR}"
    s = (
        SparkSession.builder.master("local[1]")
        .appName("s3-connector-smoke")
        .config("spark.jars", JAR)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
        .getOrCreate()
    )
    yield s
    s.stop()


def _scala_options_class(spark):
    return spark._jvm.com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions


def _parse(spark, opts):
    """Run the connector's own Scala option parser on a Python dict."""
    jmap = spark._jvm.java.util.HashMap()
    for k, v in opts.items():
        jmap.put(k, v)
    smap = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(jmap)
    return _scala_options_class(spark).apply(smap)


def test_pyspark_version_matches_matrix():
    expected = os.environ.get("EXPECT_PYSPARK")
    if expected:
        assert pyspark.__version__ == expected


@needs_jar
def test_short_name_resolves(spark):
    df = c.read_stream(spark, SCHEMA, **REQUIRED)
    assert df.isStreaming
    assert df.schema.fieldNames() == ["valString", "valInt", "part"]


@needs_jar
def test_provider_class_resolves(spark):
    df = spark.readStream.format(c.PROVIDER_CLASS).schema(SCHEMA) \
        .options(**c.s3_connector_options(**REQUIRED)).load()
    assert df.isStreaming


@needs_jar
def test_load_without_schema_is_rejected(spark):
    with pytest.raises(Exception, match="empty schema"):
        spark.readStream.format(c.FORMAT).options(**c.s3_connector_options(**REQUIRED)).load()


@needs_jar
def test_python_keys_equal_scala_constants(spark):
    jo = _scala_options_class(spark)
    names = [
        "BASE_PATH", "MAX_FILES_PER_TRIGGER", "PATH_GLOB_FILTER", "S3_FILE_FORMAT", "MAX_FILE_AGE",
        "PARTITION_COLUMNS", "QUEUE_URL", "QUEUE_REGION", "QUEUE_TYPE",
        "QUEUE_FETCH_WAIT_TIMEOUT_SECONDS", "REPROCESS_START_BATCH_ID", "REPROCESS_END_BATCH_ID",
        "REPROCESS_DRY_RUN", "SQS_LONG_POLLING_WAIT_TIME_SECONDS", "SQS_VISIBILITY_TIMEOUT_SECONDS",
        "SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR", "SQS_MAX_RETRIES", "SQS_MAX_CONCURRENCY",
    ]
    jvm_keys = {n: getattr(jo, n)() for n in names}
    if "..sqs." in jvm_keys["SQS_MAX_RETRIES"]:
        pytest.fail("this jar predates the SQS key fix: it reads only spark.s3conn..sqs.* keys")
    assert {n: getattr(c, n) for n in names} == jvm_keys


@needs_jar
def test_scala_parser_reads_helper_options(spark):
    opts = c.s3_connector_options(
        **REQUIRED,
        max_files_per_trigger=7,
        max_file_age="2h",
        path_glob_filter="*.csv",
        partition_columns=["part"],
        queue_fetch_wait_timeout_seconds=11,
        reprocess_start_batch_id=1,
        reprocess_end_batch_id=2,
        reprocess_dry_run=False,
        sqs_long_polling_wait_time_seconds=3,
        sqs_max_concurrency=4,
        sqs_max_retries=5,
        sqs_visibility_timeout_seconds=66,
        sqs_keep_message_for_consumer_error=True,
    )
    p = _parse(spark, opts)
    assert p.fileFormat() == "csv"
    assert p.queueUrl() == REQUIRED["queue_url"]
    assert p.queueRegion() == "us-east-1"
    assert p.queueType() == "SQS"
    assert p.maxFilesPerTrigger().get() == 7
    assert p.maxFileAgeMs() == 2 * 3600 * 1000
    assert p.pathGlobFilter().get() == "*.csv"
    assert p.partitionColumns().get() == "part"
    assert p.queueFetchWaitTimeoutSeconds() == 11
    assert p.reprocessStartBatchId().get() == 1
    assert p.reprocessEndBatchId().get() == 2
    assert p.reprocessState().toString() == "InAction"
    # One-dot SQS keys: these differ from the defaults (10, 50, 10, 60, false).
    assert p.sqsLongPollWaitTimeSeconds() == 3
    assert p.sqsMaxConcurrency() == 4
    assert p.sqsMaxRetries() == 5
    assert p.sqsVisibilityTimeoutSeconds() == 66
    assert p.sqsKeepMessageForConsumerError() is True


@needs_jar
@pytest.mark.parametrize(
    "drop, bad, message",
    [
        ("spark.s3conn.fileFormat", {}, "fileFormat is mandatory"),
        ("spark.s3conn.queueUrl", {}, "queueUrl is not specified"),
        (None, {"spark.s3conn.reprocessStartBatchId": "-1"}, "reprocessStartBatchId"),
        (None, {"spark.s3conn.sqs.longPollingWaitTimeSeconds": "99"}, "longPollingWaitTimeSeconds"),
        (None, {"spark.s3conn.sqs.maxRetries": "x"}, "maxRetries"),
    ],
)
def test_scala_parser_rejects_bad_options(spark, drop, bad, message):
    opts = {**c.s3_connector_options(**REQUIRED), **bad}
    opts.pop(drop, None)
    with pytest.raises(Exception, match=message):
        _parse(spark, opts)


@needs_jar
@pytest.mark.parametrize(
    "drop, bad, message",
    [
        ("spark.s3conn.fileFormat", {}, "fileFormat is mandatory"),
        (None, {"spark.s3conn.queueType": "KAFKA"}, "Unsupported queue type"),
    ],
)
def test_started_query_rejects_bad_options(spark, tmp_path, drop, bad, message):
    # Options built without the helper, so only the Scala side checks them.
    opts = {**c.s3_connector_options(**REQUIRED), **bad}
    opts.pop(drop, None)
    df = spark.readStream.format(c.FORMAT).schema(SCHEMA).options(**opts).load()
    q = df.writeStream.format("noop").option("checkpointLocation", str(tmp_path / "cp")).start()
    try:
        with pytest.raises(Exception, match=message):
            q.awaitTermination(120)
    finally:
        q.stop()


def test_missing_jar_gives_hint(tmp_path):
    # A fresh JVM without the jar, so this test does not share the session above.
    code = textwrap.dedent(
        """
        import spark_s3_connector as c
        from pyspark.sql import SparkSession
        s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
        try:
            c.read_stream(s, "a STRING", file_format="csv", queue_url="u", queue_region="r")
        except c.ConnectorNotFoundError as e:
            print("HINT:", e)
        finally:
            s.stop()
        """
    )
    env = {k: v for k, v in os.environ.items() if k != "CONNECTOR_JAR"}
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, timeout=300,
    ).stdout
    scala = c.scala_binary_version(pyspark.__version__)
    assert "HINT: The s3-connector connector jar is not on the Spark classpath" in out, out[-3000:]
    assert f"Scala {scala}" in out
