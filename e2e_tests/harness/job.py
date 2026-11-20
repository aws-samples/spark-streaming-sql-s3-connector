"""One EMR Serverless run of job/stream_job.py, and how to watch and stop it."""
from __future__ import annotations

import json
import re
import time
from typing import Callable, Dict, List, Optional

from .aws import Stack
from .events import wait_until

TERMINAL = {"SUCCESS", "FAILED", "CANCELLED"}
_BATCH_DIR = re.compile(r"/batches/batch=(\d+)/")

# Smallest useful footprint: one driver and one executor, 1 vCPU each.
BASE_SPARK_CONF = {
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.instances": "1",
    "spark.driver.cores": "1",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.sql.shuffle.partitions": "2",
    # The job uses no tables; keep Spark off the Glue Data Catalog (the role has no Glue rights).
    "spark.sql.catalogImplementation": "in-memory",
}


class JobFailed(AssertionError):
    pass


class JobRun:
    """config is the stream_job.py config without the paths; paths are filled in here."""

    def __init__(self, stack: Stack, artifacts: Dict[str, str], prefix: str, name: str,
                 config: Dict, spark_conf: Optional[Dict[str, str]] = None,
                 timeout_minutes: int = 20):
        self.stack = stack
        self.artifacts = artifacts
        self.prefix = "%s/%s" % (prefix.rstrip("/"), name)  # run/<scenario>/<session>/<name>
        self.name = name
        self.config = dict(config)
        self.config.setdefault("output", stack.s3_uri(self.prefix))
        self.spark_conf = dict(BASE_SPARK_CONF, **(spark_conf or {}))
        self.timeout_minutes = timeout_minutes
        self.job_run_id: Optional[str] = None

    # --- lifecycle ----------------------------------------------------------
    def start(self) -> "JobRun":
        # sparkSubmitParameters is one space-separated string; a space in a value would split it.
        for kv in self.spark_conf.items():
            assert not any(c.isspace() for c in "".join(kv)), "whitespace in spark conf %r" % (kv,)
        config_key = self.prefix + "/config.json"
        self.stack.put(config_key, json.dumps(self.config).encode())
        params = "--jars %s %s" % (self.artifacts["jar"], " ".join(
            "--conf %s=%s" % kv for kv in sorted(self.spark_conf.items())))
        resp = self.stack.emr.start_job_run(
            applicationId=self.stack.app_id,
            executionRoleArn=self.stack.job_role_arn,
            name=("e2e-" + self.prefix.replace("/", "-"))[:256],
            executionTimeoutMinutes=self.timeout_minutes,
            jobDriver={"sparkSubmit": {
                "entryPoint": self.artifacts["job"],
                "entryPointArguments": [self.stack.s3_uri(config_key)],
                "sparkSubmitParameters": params,
            }},
            configurationOverrides={"monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": self.stack.s3_uri("logs/")}}},
        )
        self.job_run_id = resp["jobRunId"]
        return self

    def state(self) -> Dict:
        return self.stack.emr.get_job_run(applicationId=self.stack.app_id,
                                          jobRunId=self.job_run_id)["jobRun"]

    def _check_alive(self) -> None:
        run = self.state()
        if run["state"] in TERMINAL and not self.stack.exists(self.prefix + "/status.json"):
            raise JobFailed("job %s ended %s without status.json: %s. Logs: %s" % (
                self.job_run_id, run["state"], run.get("stateDetails", ""),
                self.stack.s3_uri("logs/applications/%s/jobs/%s/" % (self.stack.app_id, self.job_run_id))))

    def wait_started(self, timeout_s: float = 600) -> None:
        """Wait until the streaming query is running (or the run already wrote its status)."""
        def ready():
            self._check_alive()
            return (self.stack.exists(self.prefix + "/started.json")
                    or self.stack.exists(self.prefix + "/status.json"))
        wait_until(ready, timeout_s, 10, "job %s to start" % self.name)

    def wait_for(self, predicate: Callable[[List[Dict]], bool], timeout_s: float = 300,
                 what: str = "batches") -> List[Dict]:
        """Poll the batch manifests until predicate(manifests) is true."""
        def check():
            m = self.manifests()
            if predicate(m):
                return m or [{}]
            self._check_alive()
            if self.stack.exists(self.prefix + "/status.json"):
                raise JobFailed("job %s stopped before %s: %s" % (self.name, what, self.status()))
            return None
        result = wait_until(check, timeout_s, 5, "%s in %s" % (what, self.name))
        return [m for m in result if m]

    def wait_rows(self, n: int, timeout_s: float = 300) -> List[Dict]:
        return self.wait_for(lambda m: sum(x["rows"] for x in m) >= n, timeout_s, "%d rows" % n)

    def wait_done(self, timeout_s: float = 900) -> Dict:
        """Wait for the run to end by itself; return status.json."""
        def done():
            return self.state()["state"] in TERMINAL
        wait_until(done, timeout_s, 10, "job %s to end" % self.name)
        self._check_alive()
        return self.status()

    def stop(self, timeout_s: float = 600) -> Dict:
        """Ask the query to stop (STOP marker) and wait for status.json."""
        self.stack.put(self.prefix + "/STOP", b"")
        return self.wait_done(timeout_s)

    def wait_crashed(self, timeout_s: float = 900) -> None:
        """Wait for a run started with crash_after_batch to die (FAILED, no status.json)."""
        def crashed():
            if self.stack.exists(self.prefix + "/status.json"):  # ended cleanly: the crash never fired
                raise JobFailed("job %s did not crash: %s" % (self.name, self.status()))
            return self.state()["state"] in TERMINAL
        wait_until(crashed, timeout_s, 10, "crash of %s" % self.name)
        run = self.state()
        assert run["state"] == "FAILED" and not self.stack.exists(self.prefix + "/status.json"), run

    def cleanup(self) -> None:
        """Cancel a run that is still going (test failed half way). Never raises."""
        if not self.job_run_id:
            return
        try:
            if self.state()["state"] not in TERMINAL:
                self.stack.emr.cancel_job_run(applicationId=self.stack.app_id, jobRunId=self.job_run_id)
        except Exception:  # noqa: BLE001 - teardown must not mask the test result
            pass

    # --- results ------------------------------------------------------------
    def status(self) -> Dict:
        return self.stack.get_json(self.prefix + "/status.json")

    def manifests(self) -> List[Dict]:
        keys = [k for k in self.stack.list_keys(self.prefix + "/manifests/") if k.endswith(".json")]
        return sorted((self.stack.get_json(k) for k in keys), key=lambda m: m["batch_id"])

    def records_by_batch(self) -> Dict[int, List[Dict]]:
        """Output rows per batch id, only for batches whose manifest exists (write finished).

        An interrupted write may leave part files behind; the manifest is written after them.
        """
        done = {m["batch_id"] for m in self.manifests()}
        out: Dict[int, List[Dict]] = {}
        for key in self.stack.list_keys(self.prefix + "/batches/"):
            m = _BATCH_DIR.search(key)
            if m and int(m.group(1)) in done and key.rsplit("/", 1)[-1].startswith("part-"):
                out.setdefault(int(m.group(1)), []).extend(
                    json.loads(line) for line in self.stack.get(key).decode().splitlines() if line)
        return out

    def records(self) -> List[Dict]:
        """All output rows of all batches."""
        return [r for rows in self.records_by_batch().values() for r in rows]


def wait_quiet(seconds: float) -> None:
    """Give the stream a few triggers with nothing new, to prove nothing else arrives."""
    time.sleep(seconds)
