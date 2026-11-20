"""Per-test view of one scenario: its prefix, queue, checkpoint and job runs."""
from __future__ import annotations

from typing import Dict, List, Optional

from .aws import Stack, assert_queues_drained
from .events import base_options, py_base_options
from .fixtures import SCHEMA_DDL
from .job import JobRun, wait_quiet


class Scenario:
    def __init__(self, stack: Stack, artifacts: Dict[str, str], name: str, session_id: str):
        q = stack.queue(name)
        self.stack = stack
        self.artifacts = artifacts
        self.name = name
        self.queue_url = q["queue_url"]
        self.dlq_url = q["dlq_url"]
        self.input_prefix = q["prefix"]
        self.run_prefix = "run/%s/%s" % (name, session_id)
        self.checkpoint_prefix = self.run_prefix + "/checkpoint"
        self.checkpoint = stack.s3_uri(self.checkpoint_prefix)
        self.jobs: List[JobRun] = []

    def reset(self) -> "Scenario":
        self.stack.reset_scenario(self.name)
        return self

    def key(self, rel: str) -> str:
        return self.input_prefix + rel

    def upload(self, rel: str, body: bytes) -> str:
        key = self.key(rel)
        self.stack.put(key, body)
        return key

    def uri(self, rel: str) -> str:
        return self.stack.s3_uri(self.key(rel))

    def options(self, fmt: str, **extra: str) -> Dict[str, str]:
        return base_options(self.queue_url, self.stack.region, fmt, **extra)

    def py_options(self, fmt: str, **extra) -> Dict:
        """read_stream() kwargs for this scenario's queue, same values as base_options."""
        return py_base_options(self.queue_url, self.stack.region, fmt, **extra)

    def job(self, name: str, source_options: Dict[str, str], schema: Optional[str] = SCHEMA_DDL,
            checkpoint: Optional[str] = None, spark_conf: Optional[Dict[str, str]] = None,
            pyspark: bool = False, **cfg) -> JobRun:
        """pyspark=True: source_options are read_stream() kwargs (see py_options)."""
        config = {"mode": "stream", "source_options": source_options, "schema": schema,
                  "checkpoint": checkpoint or self.checkpoint, "trigger_seconds": 5,
                  "max_seconds": 900}
        config.update(cfg)
        run = JobRun(self.stack, self.artifacts, self.run_prefix, name, config, spark_conf,
                     pyspark=pyspark)
        self.jobs.append(run)
        return run

    def run_until_rows(self, name: str, source_options: Dict[str, str], n_rows: int,
                       quiet_s: float = 0, **job_kw) -> JobRun:
        """Start a run, wait for n_rows output rows, wait quiet_s more, stop it cleanly."""
        run = self.job(name, source_options, **job_kw).start()
        run.wait_started()
        run.wait_rows(n_rows)
        if quiet_s:
            wait_quiet(quiet_s)
        status = run.stop()
        assert status["state"] == "stopped", status
        return run

    def assert_drained(self, timeout_s: float = 90) -> None:
        assert_queues_drained(self.stack, self.queue_url, self.dlq_url, timeout_s)

    def cleanup(self) -> None:
        for run in self.jobs:
            run.cleanup()
