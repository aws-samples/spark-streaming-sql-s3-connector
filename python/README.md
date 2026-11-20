# spark-streaming-sql-s3-connector for PySpark

Python helpers for the
[Spark Structured Streaming S3 connector](https://github.com/aws-samples/spark-streaming-sql-s3-connector).
The connector is a JVM jar. This package builds and checks its options and calls
`spark.readStream.format("s3-connector")`. It has no dependencies; PySpark comes from
your platform. One wheel works with every Spark version below.

## Install

The package is not on PyPI. Build the wheel from this folder and install it:

```
cd python
pip wheel --no-deps -w dist .
pip install dist/spark_streaming_sql_s3_connector-*.whl
```

On EMR add it with a bootstrap action or a virtualenv archive; on Glue use
`--additional-python-modules`. Python 3.7 is listed for EMR 6.x but the tests run on 3.8 and later.
Then add the connector **jar built for your Spark release**
(`spark-streaming-sql-s3-connector-0.0.3-spark<spark version>.jar`, see the main README for how to build it):

| Platform | Spark | Python | Connector jar |
|---|---|---|---|
| EMR 6.7 | 3.2.1 | 3.7 | `spark-streaming-sql-s3-connector-0.0.3-spark3.2.1.jar` |
| EMR 6.8 - 6.11, Glue 4.0 | 3.3.0 - 3.3.2 | 3.7 / 3.10 (Glue) | `spark-streaming-sql-s3-connector-0.0.3-spark3.3.<x>.jar` |
| EMR 6.12 - 6.15 | 3.4.0 - 3.4.1 | 3.7 | `spark-streaming-sql-s3-connector-0.0.3-spark3.4.<x>.jar` |
| EMR 7.0 - 7.14, Glue 5.0, 5.1 | 3.5.0 - 3.5.8 | 3.9 / 3.11 (Glue) | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.<x>.jar` |
| EMR emr-spark-8.0.0 ([known limitation](../README.md#known-limitation-emr-80)) | 4.0.2 | 3.11 (tested) | `spark-streaming-sql-s3-connector-0.0.3-spark4.0.2.jar` |
| Glue 6.0, EMR emr-spark-8.1.0 | 4.1.1 | 3.13 | `spark-streaming-sql-s3-connector-0.0.3-spark4.1.1.jar` |
| EMR emr-spark-8.2.0 | 4.1.3 | 3.13 (tested) | `spark-streaming-sql-s3-connector-0.0.3-spark4.1.3.jar` |

`scala_binary_version(spark.version)` returns `"2.12"` or `"2.13"`. Pass the jar with
`--jars` (spark-submit, EMR) or `--extra-jars` (Glue).

## Use

```python
from spark_s3_connector import read_stream

df = read_stream(
    spark,
    "valString STRING, valInt INT, part STRING",  # a schema is required
    file_format="csv",
    queue_url="https://sqs.us-east-1.amazonaws.com/<account id>/<queue name>",
    queue_region="us-east-1",
    max_files_per_trigger=500,
    partition_columns=["part"],
    base_path="s3://<bucket>/<root>/",
)
```

Or build only the options and call Spark yourself:

```python
from spark_s3_connector import FORMAT, s3_connector_options

opts = s3_connector_options(file_format="json", queue_url=url, queue_region="us-east-1")
df = spark.readStream.format(FORMAT).schema(schema).options(**opts).load()
```

`s3_connector_options` raises `ValueError` / `TypeError` at once for options the connector
would reject only when the query starts. File-format options (e.g. `header`) go in
`extra_options={...}`. If the jar is missing, `read_stream` raises `ConnectorNotFoundError`.

Set `base_path` to the full `s3://<bucket>/<prefix>/`. Since connector 0.0.3 it is also a filter:
messages for files outside it are skipped (and deleted unless
`sqs_keep_message_for_consumer_error=True`). `path_glob_filter` matches the full `s3://` path.

AWS credentials come from the default AWS credential chain on the driver and executors
(instance profile, EMR or Glue job role). The helper has no credential options and adds
none. Do not pass keys in `extra_options`: query options can show up in logs and plans.

Full example: `examples/read_stream.py`.

### SQS option keys

The helper writes the SQS keys with one dot, e.g. `spark.s3conn.sqs.maxRetries`. Connector
jars after 0.0.2 read these first and still accept the old two-dot spelling
(`spark.s3conn..sqs.maxRetries`). Jars up to 0.0.2 read only the two-dot keys and silently
use their defaults for the one-dot ones: with such a jar, pass the two-dot keys in
`extra_options`.

## Test

```
cd python
pip install -e .[test] && pytest tests/test_options.py                          # no Spark
CONNECTOR_JAR=<jar> pytest                                                       # one Spark
python tests/run_matrix.py --work .work --jar 3.5=spark-streaming-sql-s3-connector-0.0.3-spark3.5.8.jar ...   # all versions
```
