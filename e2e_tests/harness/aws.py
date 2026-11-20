"""Thin boto3 layer over the Terraform stack: S3 objects, SQS queues, checkpoint state."""
from __future__ import annotations

import json
from typing import Dict, List, Optional

import boto3

from . import checkpoint
from .events import wait_until


class Stack:
    """The resources from `terraform output -json`, plus clients for them."""

    def __init__(self, outputs: Dict, release_tag: str, session: Optional[boto3.Session] = None):
        self.outputs = outputs
        self.region = outputs["region"]
        self.bucket = outputs["bucket"]
        self.job_role_arn = outputs["job_role_arn"]
        self.queues = outputs["queues"]
        self.max_receive_count = int(outputs["max_receive_count"])
        apps = outputs["applications"]
        if release_tag not in apps:
            raise KeyError("release tag %r not in Terraform applications %s" % (release_tag, sorted(apps)))
        self.release_tag = release_tag
        self.app_id = apps[release_tag]["id"]
        self.release_label = apps[release_tag]["release_label"]
        self.remote = outputs.get("remote")
        session = session or boto3.Session(region_name=self.region)
        self.s3 = session.client("s3")
        self.sqs = session.client("sqs")
        self.emr = session.client("emr-serverless")

    def queue(self, name: str) -> Dict:
        if name not in self.queues:
            raise KeyError("scenario %r has no queue; is it in e2e_tests/scenarios.py and applied?" % name)
        return self.queues[name]

    def s3_uri(self, key: str) -> str:
        return "s3://%s/%s" % (self.bucket, key)

    # --- S3 -----------------------------------------------------------------
    def put(self, key: str, body: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)

    def get(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def get_json(self, key: str):
        return json.loads(self.get(key))

    def exists(self, key: str) -> bool:
        return bool(self.list_keys(key))

    def list_keys(self, prefix: str) -> List[str]:
        keys: List[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", [])]
        return keys

    def delete_prefix(self, prefix: str) -> None:
        keys = self.list_keys(prefix)
        for i in range(0, len(keys), 1000):
            self.s3.delete_objects(Bucket=self.bucket,
                                   Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})

    # --- SQS ----------------------------------------------------------------
    def counts(self, queue_url: str) -> int:
        """Visible + in flight + delayed messages (SQS approximate counts)."""
        attrs = self.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        return sum(int(v) for v in attrs.values())

    def send(self, queue_url: str, body: str) -> None:
        self.sqs.send_message(QueueUrl=queue_url, MessageBody=body)

    def receive_all(self, queue_url: str) -> List[Dict]:
        """Receive (and delete) every message currently in the queue."""
        got: List[Dict] = []
        empty = 0
        while empty < 2:
            msgs = self.sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10,
                                            WaitTimeSeconds=1, VisibilityTimeout=30).get("Messages", [])
            if not msgs:
                empty += 1
                continue
            empty = 0
            got += msgs
            self.sqs.delete_message_batch(QueueUrl=queue_url, Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)])
        return got

    def wait_visible(self, queue_url: str, n: int, timeout_s: float = 120) -> None:
        """Wait until at least n messages sit in the queue (S3 events arrive within seconds)."""
        wait_until(lambda: self.counts(queue_url) >= n, timeout_s, 3, "%d messages in %s" % (n, queue_url))

    def reset_scenario(self, name: str) -> None:
        """Empty the scenario's queue and DLQ (drops the s3:TestEvent from apply) and its prefix."""
        q = self.queue(name)
        self.receive_all(q["queue_url"])
        self.receive_all(q["dlq_url"])
        self.delete_prefix(q["prefix"])

    # --- checkpoint ---------------------------------------------------------
    def checkpoint_state(self, checkpoint_prefix: str) -> Dict:
        """Offsets, commits and connector RocksDB state of a checkpoint under s3://bucket/<prefix>."""
        p = checkpoint_prefix.rstrip("/") + "/"
        offsets = checkpoint.batch_ids(self.list_keys(p + "offsets/"))
        commits = checkpoint.batch_ids(self.list_keys(p + "commits/"))
        versions = checkpoint.rocksdb_versions(self.list_keys(p + "sources/0/"))
        state = {"offsets": offsets, "commits": commits, "rocksdb_versions": versions,
                 "log_offset": None, "num_keys": None,
                 "sst_files": len(self.list_keys(p + "sources/0/SSTs/")),
                 "reprocess_lock": self.exists(p + "sources/0/reprocess.lock")}
        if offsets:
            state["log_offset"] = checkpoint.source_log_offset(
                self.get(p + "offsets/%d" % offsets[-1]).decode())
        if versions:
            state["num_keys"] = checkpoint.rocksdb_zip_metadata(
                self.get(p + "sources/0/%d.zip" % versions[-1]))["numKeys"]
        return state


def assert_queues_drained(stack: Stack, queue_url: str, dlq_url: str, timeout_s: float = 90) -> None:
    """Every message was deleted by the connector and none went to the DLQ."""
    wait_until(lambda: stack.counts(queue_url) == 0, timeout_s, 5, "queue %s to drain" % queue_url)
    assert stack.counts(dlq_url) == 0, "messages reached the DLQ %s" % dlq_url
