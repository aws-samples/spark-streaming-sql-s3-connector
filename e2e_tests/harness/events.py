"""Pure helpers: connector option names, synthetic S3 event messages, polling.

No AWS calls here, so this module is unit-tested offline (e2e_tests/unit).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, TypeVar
from urllib.parse import quote_plus

T = TypeVar("T")

PREFIX = "spark.s3conn."
# The two spellings of the sqs.* keys: one dot is the documented form; two dots is the
# legacy form of releases up to 0.0.2, still accepted (one dot wins). Tests run both.
SQS_PREFIXES = {
    "double_dot": PREFIX + ".sqs.",
    "single_dot": PREFIX + "sqs.",
}

SOURCE_FORMAT = "s3-connector"


def sqs_key(form: str, name: str) -> str:
    """Full option key for an sqs.* setting in the given spelling."""
    return SQS_PREFIXES[form] + name


def base_options(queue_url: str, queue_region: str, file_format: str,
                 sqs_form: str = "double_dot", **extra: str) -> Dict[str, str]:
    """Options every positive test starts from: short polling so tests stay fast.

    longPollingWaitTimeSeconds=2 and queueFetchWaitTimeoutSeconds=4 keep each trigger
    short; visibilityTimeoutSeconds=30 is long enough for a batch to commit. maxConcurrency
    and maxRetries are set (to small values) so every positive run exercises them too.
    """
    opts = {
        PREFIX + "queueUrl": queue_url,
        PREFIX + "queueRegion": queue_region,
        PREFIX + "fileFormat": file_format,
        PREFIX + "queueFetchWaitTimeoutSeconds": "4",
        sqs_key(sqs_form, "longPollingWaitTimeSeconds"): "2",
        sqs_key(sqs_form, "visibilityTimeoutSeconds"): "30",
        sqs_key(sqs_form, "maxConcurrency"): "5",
        sqs_key(sqs_form, "maxRetries"): "3",
    }
    opts.update(extra)
    return opts


def s3_event_time(ts: datetime) -> str:
    """S3 eventTime format; the connector parses exactly yyyy-MM-dd'T'HH:mm:ss.SSS'Z'."""
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (ts.microsecond // 1000)


def s3_event_body(bucket: str, key: str, event_time: datetime,
                  event_name: str = "ObjectCreated:Put", size: int = 1) -> str:
    """A minimal S3 event notification body, as S3 sends it (key URL-encoded, space as '+')."""
    record = {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "eventTime": s3_event_time(event_time),
        "eventName": event_name,
        "s3": {
            "bucket": {"name": bucket, "arn": "arn:aws:s3:::" + bucket},
            "object": {"key": quote_plus(key, safe="/"), "size": size},
        },
    }
    return json.dumps({"Records": [record]})


def s3_test_event_body(bucket: str) -> str:
    """The s3:TestEvent S3 sends when a notification is configured (no Records)."""
    return json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": bucket,
                       "Time": s3_event_time(datetime.now(timezone.utc))})


def wait_until(fn: Callable[[], Optional[T]], timeout_s: float, interval_s: float = 5.0,
               what: str = "condition") -> T:
    """Call fn until it returns a truthy value or timeout_s passes; return that value."""
    deadline = time.monotonic() + timeout_s
    while True:
        value = fn()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out after %.0fs waiting for %s" % (timeout_s, what))
        time.sleep(interval_s)


def exception_text(chain: list) -> str:
    """Flatten a status.json exception chain into one searchable string."""
    return "\n".join("%s: %s" % (e.get("class"), e.get("message")) for e in chain or [])
