"""PySpark helpers for the Spark Structured Streaming S3 connector.

The connector itself is a JVM jar. This package only builds the option map the
jar reads and calls ``spark.readStream.format("s3-connector")``.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, Mapping, Optional, Union

__all__ = [
    "FORMAT",
    "PROVIDER_CLASS",
    "ConnectorNotFoundError",
    "s3_connector_options",
    "read_stream",
    "scala_binary_version",
]

__version__ = "0.0.3"  # matches the first connector jar that reads the one-dot SQS keys

FORMAT = "s3-connector"
PROVIDER_CLASS = "com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceProvider"

# Option keys, as in S3ConnectorSourceOptions.scala.
_P = "spark.s3conn."
# One-dot SQS keys: read first by the fixed connector. Jars up to 0.0.2 read only
# the two-dot spelling (spark.s3conn..sqs.*) and ignore these.
_SQS = _P + "sqs."

BASE_PATH = "basePath"
MAX_FILES_PER_TRIGGER = _P + "maxFilesPerTrigger"
PATH_GLOB_FILTER = _P + "pathGlobFilter"
S3_FILE_FORMAT = _P + "fileFormat"
MAX_FILE_AGE = _P + "maxFileAge"
PARTITION_COLUMNS = _P + "partitionColumns"
QUEUE_URL = _P + "queueUrl"
QUEUE_REGION = _P + "queueRegion"
QUEUE_TYPE = _P + "queueType"
QUEUE_FETCH_WAIT_TIMEOUT_SECONDS = _P + "queueFetchWaitTimeoutSeconds"
REPROCESS_START_BATCH_ID = _P + "reprocessStartBatchId"
REPROCESS_END_BATCH_ID = _P + "reprocessEndBatchId"
REPROCESS_DRY_RUN = _P + "reprocessDryRun"
SQS_LONG_POLLING_WAIT_TIME_SECONDS = _SQS + "longPollingWaitTimeSeconds"
SQS_VISIBILITY_TIMEOUT_SECONDS = _SQS + "visibilityTimeoutSeconds"
SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR = _SQS + "keepMessageForConsumerError"
SQS_MAX_RETRIES = _SQS + "maxRetries"
SQS_MAX_CONCURRENCY = _SQS + "maxConcurrency"

# JavaUtils.timeStringAsMs: an integer with an optional unit (default ms).
_TIME_RE = re.compile(r"^([0-9]+)(us|ms|s|m|min|h|d)?$")


class ConnectorNotFoundError(RuntimeError):
    """The connector jar is not on the Spark classpath."""


_INT_MAX = 2**31 - 1  # Scala's String.toInt
_LONG_MAX = 2**63 - 1  # Java's Long.parseLong


def _int(name: str, value: object, minimum: int, maximum: int = _INT_MAX) -> str:
    # bool is an int subclass; True would silently become "True".
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}, got {value}")
    return str(value)


def _bool(name: str, value: object) -> str:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, got {type(value).__name__}")
    return "true" if value else "false"


def _str(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def s3_connector_options(
    *,
    file_format: str,
    queue_url: str,
    queue_region: str,
    queue_type: str = "SQS",
    max_files_per_trigger: Optional[int] = None,
    max_file_age: Optional[str] = None,
    path_glob_filter: Optional[str] = None,
    partition_columns: Union[str, Iterable[str], None] = None,
    base_path: Optional[str] = None,
    queue_fetch_wait_timeout_seconds: Optional[int] = None,
    reprocess_start_batch_id: Optional[int] = None,
    reprocess_end_batch_id: Optional[int] = None,
    reprocess_dry_run: Optional[bool] = None,
    sqs_long_polling_wait_time_seconds: Optional[int] = None,
    sqs_max_concurrency: Optional[int] = None,
    sqs_max_retries: Optional[int] = None,
    sqs_visibility_timeout_seconds: Optional[int] = None,
    sqs_keep_message_for_consumer_error: Optional[bool] = None,
    extra_options: Optional[Mapping[str, str]] = None,
) -> Dict[str, str]:
    """Return the option map for ``spark.readStream.format("s3-connector")``.

    Checks the same rules as the Scala parser, which only runs when the query
    starts. Unset options are left out, so the connector's defaults apply.
    ``extra_options`` (e.g. Spark file-format options) are added unchanged and
    may not override a key set by a named argument.
    """
    opts = {
        S3_FILE_FORMAT: _str("file_format", file_format),
        QUEUE_URL: _str("queue_url", queue_url),
        QUEUE_REGION: _str("queue_region", queue_region),
    }
    if queue_type != "SQS":
        raise ValueError(f"queue_type must be 'SQS' (the only supported type), got {queue_type!r}")
    opts[QUEUE_TYPE] = queue_type

    if max_files_per_trigger is not None:
        # -1 means unlimited (the Scala parser maps any value <= 0 to unlimited).
        if max_files_per_trigger != -1:
            _int("max_files_per_trigger (or -1 for unlimited)", max_files_per_trigger, 1)
        opts[MAX_FILES_PER_TRIGGER] = str(max_files_per_trigger)
    if max_file_age is not None:
        m = _TIME_RE.match(max_file_age.strip().lower()) if isinstance(max_file_age, str) else None
        if not m or int(m.group(1)) > _LONG_MAX:
            raise ValueError(
                "max_file_age must be a duration like '15d', '12h', '30m', '90s' or '500ms', "
                f"got {max_file_age!r}"
            )
        opts[MAX_FILE_AGE] = max_file_age.strip()
    if path_glob_filter is not None:
        opts[PATH_GLOB_FILTER] = _str("path_glob_filter", path_glob_filter)
    if partition_columns is not None:
        cols = [partition_columns] if isinstance(partition_columns, str) else list(partition_columns)
        cols = [c.strip() for col in cols for c in _str("partition_columns", col).split(",")]
        if not all(cols):
            raise ValueError("partition_columns must not contain empty names")
        opts[PARTITION_COLUMNS] = ",".join(cols)
    if base_path is not None:
        opts[BASE_PATH] = _str("base_path", base_path)
    if queue_fetch_wait_timeout_seconds is not None:
        opts[QUEUE_FETCH_WAIT_TIMEOUT_SECONDS] = _int(
            "queue_fetch_wait_timeout_seconds", queue_fetch_wait_timeout_seconds, 1
        )

    if (reprocess_start_batch_id is None) != (reprocess_end_batch_id is None):
        # The Scala parser accepts one alone but then does not reprocess at all.
        raise ValueError("set both reprocess_start_batch_id and reprocess_end_batch_id, or neither")
    if reprocess_start_batch_id is not None:
        opts[REPROCESS_START_BATCH_ID] = _int("reprocess_start_batch_id", reprocess_start_batch_id, 0)
        opts[REPROCESS_END_BATCH_ID] = _int("reprocess_end_batch_id", reprocess_end_batch_id, 0)
        if reprocess_start_batch_id > reprocess_end_batch_id:
            raise ValueError("reprocess_start_batch_id must be <= reprocess_end_batch_id")
    if reprocess_dry_run is not None:
        opts[REPROCESS_DRY_RUN] = _bool("reprocess_dry_run", reprocess_dry_run)

    if sqs_long_polling_wait_time_seconds is not None:
        opts[SQS_LONG_POLLING_WAIT_TIME_SECONDS] = _int(
            "sqs_long_polling_wait_time_seconds", sqs_long_polling_wait_time_seconds, 0, 20
        )
    if sqs_max_concurrency is not None:
        opts[SQS_MAX_CONCURRENCY] = _int("sqs_max_concurrency", sqs_max_concurrency, 1)
    if sqs_max_retries is not None:
        opts[SQS_MAX_RETRIES] = _int("sqs_max_retries", sqs_max_retries, 1)
    if sqs_visibility_timeout_seconds is not None:
        opts[SQS_VISIBILITY_TIMEOUT_SECONDS] = _int(
            "sqs_visibility_timeout_seconds", sqs_visibility_timeout_seconds, 1
        )
    if sqs_keep_message_for_consumer_error is not None:
        opts[SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR] = _bool(
            "sqs_keep_message_for_consumer_error", sqs_keep_message_for_consumer_error
        )

    taken = {k.lower() for k in opts}  # Spark looks options up case-insensitively
    for key, value in (extra_options or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("extra_options keys and values must be strings")
        if key.lower() in taken:
            raise ValueError(f"extra_options must not override {key!r}; use the named argument")
        opts[key] = value
    return opts


def scala_binary_version(spark_version: str) -> str:
    """Scala version of the connector jar to use with this Spark version."""
    try:
        major = int(spark_version.split(".", 1)[0])
    except ValueError:
        raise ValueError(f"not a Spark version: {spark_version!r}") from None
    if major == 3:
        return "2.12"
    if major == 4:
        return "2.13"
    raise ValueError(f"unsupported Spark version {spark_version!r}; supported: 3.2.1 and later 3.x, 4.x")


def read_stream(spark, schema, **options):
    """``spark.readStream.format("s3-connector")`` with checked options.

    ``schema`` is required (a ``StructType`` or DDL string): the connector does
    not infer one. ``options`` are the keyword arguments of
    :func:`s3_connector_options`.
    """
    if schema is None:
        raise ValueError("schema is required: the s3-connector source does not infer a schema")
    opts = s3_connector_options(**options)
    try:
        return spark.readStream.format(FORMAT).schema(schema).options(**opts).load()
    except Exception as e:  # py4j and Spark Connect raise different types
        msg = str(e)
        if "Failed to find data source" in msg or "Failed to find the data source" in msg \
                or "DATA_SOURCE_NOT_FOUND" in msg:
            try:
                which = f"Spark {spark.version} (Scala {scala_binary_version(spark.version)})"
            except ValueError:
                which = f"Spark {spark.version}"
            raise ConnectorNotFoundError(
                f"The {FORMAT} connector jar is not on the Spark classpath. Add the jar built for "
                f"{which} with --jars (spark-submit, EMR) or --extra-jars (Glue)."
            ) from e
        raise
