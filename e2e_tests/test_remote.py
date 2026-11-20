"""UC-28, UC-29: bucket and queue in another region and/or account (terraform var.remote)."""
from collections import Counter

import boto3
import pytest

from harness.events import base_options, wait_until
from harness.fixtures import file_bytes, output_tuple, rows_for
from harness.job import JobRun

pytestmark = pytest.mark.e2e


def _remote_session(remote, home_region):
    if not remote.get("assume_role_arn"):
        return boto3.Session(region_name=remote["region"])
    creds = boto3.client("sts", region_name=home_region).assume_role(RoleArn=remote["assume_role_arn"],
                                            RoleSessionName="s3conn-e2e-harness")["Credentials"]
    return boto3.Session(aws_access_key_id=creds["AccessKeyId"], aws_secret_access_key=creds["SecretAccessKey"],
                         aws_session_token=creds["SessionToken"], region_name=remote["region"])


def _count(sqs, url):
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
        "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    return sum(int(v) for v in attrs.values())


def test_remote_source(stack, artifacts, session_id):
    remote = stack.remote
    if not remote:
        pytest.skip("terraform var.remote.enabled is false")
    session = _remote_session(remote, stack.region)
    s3, sqs = session.client("s3"), session.client("sqs")

    # Start clean: drop the s3:TestEvent and anything left from an earlier run.
    sqs.purge_queue(QueueUrl=remote["queue_url"])
    wait_until(lambda: _count(sqs, remote["queue_url"]) == 0, 90, 5, "remote queue purge")

    rows = rows_for(0)
    s3.put_object(Bucket=remote["bucket"], Key=remote["prefix"] + "r0.csv", Body=file_bytes("csv", rows))
    wait_until(lambda: _count(sqs, remote["queue_url"]) >= 1, 120, 3, "remote S3 event")

    opts = base_options(remote["queue_url"], remote["region"], "csv")
    prefix = "run/remote/%s" % session_id
    run = JobRun(stack, artifacts, prefix, "run", {
        "mode": "stream", "source_options": opts, "schema": "valString STRING, valBoolean BOOLEAN, valInt INT",
        "checkpoint": stack.s3_uri(prefix + "/checkpoint"), "trigger_seconds": 5, "max_seconds": 900})
    try:
        run.start().wait_started()
        run.wait_rows(len(rows))
        assert run.stop()["state"] == "stopped"
    finally:
        run.cleanup()

    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(rows)
    wait_until(lambda: _count(sqs, remote["queue_url"]) == 0, 90, 5, "remote queue to drain")
    assert _count(sqs, remote["dlq_url"]) == 0
