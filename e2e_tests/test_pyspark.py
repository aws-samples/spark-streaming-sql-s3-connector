"""UC-52..UC-56: the main use cases again, with every source built by the PySpark helper package.

The jobs run job/pyspark_job.py with python/src/spark_s3_connector on --py-files; options are
read_stream() keyword arguments, so these tests also check that the package's option keys are the
ones the connector reads. The package writes only the one-dot spark.s3conn.sqs.* keys, which jars
up to 0.0.2 ignore: run these with a 0.0.3 or later jar. Run only these with `run_e2e.py --suite pyspark` (or pytest -m pyspark).
"""
from collections import Counter

import pytest

from harness.events import exception_text, py_base_options, wait_until
from harness.fixtures import SCHEMA_DDL, TEXT_SCHEMA_DDL, expected, file_bytes, output_tuple, rows_for
from harness.job import JobRun
from harness.scenario import Scenario

pytestmark = [pytest.mark.e2e, pytest.mark.pyspark]

EXT = {"csv": "csv", "json": "json", "parquet": "parquet", "orc": "orc", "text": "txt"}


def _upload(sc, numbers, wait=True):
    for n in numbers:
        sc.upload("f%d.csv" % n, file_bytes("csv", rows_for(n)))
    if wait:  # only while nothing consumes the queue (see test_state._upload)
        sc.stack.wait_visible(sc.queue_url, len(numbers))


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet", "orc", "text"])
def test_pyspark_file_format(scenario, fmt):
    """UC-52: plain read of each file format."""
    sc = scenario("py_fmt_" + fmt)
    names = ["f0.%s" % EXT[fmt], "f1.%s" % EXT[fmt]]
    want = []
    for n, name in enumerate(names):
        rows = rows_for(n)
        sc.upload(name, file_bytes(fmt, rows))
        want += expected(fmt, rows)
    sc.stack.wait_visible(sc.queue_url, len(names))

    run = sc.run_until_rows("run", sc.py_options(fmt), len(want), pyspark=True,
                            schema=TEXT_SCHEMA_DDL if fmt == "text" else SCHEMA_DDL)

    assert Counter(output_tuple(fmt, r) for r in run.records()) == Counter(want)
    assert len({f for m in run.manifests() for f in m["files"]}) == len(names)
    sc.assert_drained()
    state = sc.stack.checkpoint_state(sc.checkpoint_prefix)
    assert state["commits"] and state["commits"][-1] == run.manifests()[-1]["batch_id"], state


def test_pyspark_partition_columns(scenario):
    """UC-53: partition_columns and base_path."""
    sc = scenario("py_partitions")
    want = []
    for n, (p1, p2) in enumerate([("A", "x"), ("A", "y"), ("B", "x")]):
        rows = rows_for(n)
        sc.upload("p1=%s/p2=%s/f%d.csv" % (p1, p2, n), file_bytes("csv", rows))
        want += [r + (p1, p2) for r in rows]
    sc.stack.wait_visible(sc.queue_url, 3)

    opts = sc.py_options("csv", partition_columns=["p1", "p2"], base_path=sc.uri(""))
    run = sc.run_until_rows("run", opts, len(want), pyspark=True,
                            schema="valString STRING, valBoolean BOOLEAN, valInt INT, p1 STRING, p2 STRING")

    assert Counter(output_tuple("csv", r, ("p1", "p2")) for r in run.records()) == Counter(want)
    sc.assert_drained()


def test_pyspark_option_keys_reach_connector(scenario):
    """UC-54: named options (max_files_per_trigger) and extra_options (CSV header) take effect."""
    sc = scenario("py_options")
    want = []
    for n in range(3):
        rows = rows_for(n)
        sc.upload("f%d.csv" % n, file_bytes("csv", rows, header=True))
        want += rows
    sc.stack.wait_visible(sc.queue_url, 3)

    opts = sc.py_options("csv", max_files_per_trigger=1, extra_options={"header": "true"})
    run = sc.run_until_rows("run", opts, len(want), quiet_s=10, pyspark=True)

    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(want)  # no header rows
    manifests = [m for m in run.manifests() if m["rows"]]
    assert len(manifests) == 3 and all(len(m["files"]) == 1 for m in manifests), manifests
    sc.assert_drained()


def build_option_cases(queue_url, region):
    """name -> (read_stream kwargs, schema, expectation), in run order.

    "reads" runs first, while its file is still pending. The package cases raise in the driver
    before Spark sees the options; the last case passes a bad one-dot SQS key through
    extra_options, which only the connector's Scala parser rejects.
    """
    base = py_base_options(queue_url, region, "csv")
    no_retries = {k: v for k, v in base.items() if k != "sqs_max_retries"}
    return {
        "reads": (base, SCHEMA_DDL, "reads"),
        "pkg_no_schema": (base, None, ["ValueError", "schema is required"]),
        "pkg_long_poll_out_of_range": (dict(base, sqs_long_polling_wait_time_seconds=30), SCHEMA_DDL,
                                       ["ValueError", "sqs_long_polling_wait_time_seconds must be between 0 and 20"]),
        "pkg_max_files_not_int": (dict(base, max_files_per_trigger="2"), SCHEMA_DDL,
                                  ["TypeError", "max_files_per_trigger"]),
        "pkg_reprocess_start_only": (dict(base, reprocess_start_batch_id=0), SCHEMA_DDL,
                                     ["ValueError", "set both reprocess_start_batch_id"]),
        "pkg_queue_type": (dict(base, queue_type="Kafka"), SCHEMA_DDL, ["ValueError", "queue_type must be 'SQS'"]),
        "scala_rejects_one_dot_key": (dict(no_retries, extra_options={"spark.s3conn.sqs.maxRetries": "0"}),
                                      SCHEMA_DDL, ["IllegalArgumentException",
                                                   "Invalid value '0' for option 'spark.s3conn.sqs.maxRetries'"]),
    }


OPTION_CASES = list(build_option_cases("https://q/a", "us-east-1"))


@pytest.fixture(scope="module")
def py_case_results(stack, artifacts, session_id):
    sc = Scenario(stack, artifacts, "py_option_errors", session_id).reset()
    sc.upload("f0.csv", file_bytes("csv", rows_for(0)))
    stack.wait_visible(sc.queue_url, 1)
    cases = build_option_cases(sc.queue_url, stack.region)
    config = {"mode": "cases", "cases": [
        {"name": n, "options": o, "schema": s, "seconds": 90 if isinstance(e, list) else 60}
        for n, (o, s, e) in cases.items()]}
    run = JobRun(stack, artifacts, sc.run_prefix, "cases", config, timeout_minutes=30, pyspark=True)
    try:
        status = run.start().wait_done(timeout_s=30 * 60)
    finally:
        run.cleanup()
    return cases, status["cases"]


@pytest.mark.parametrize("name", [
    pytest.param(n, marks=[pytest.mark.sqs_single_dot] if n == "scala_rejects_one_dot_key" else [], id=n)
    for n in OPTION_CASES])
def test_pyspark_option_checks(py_case_results, name):
    """UC-55: bad options fail fast in Python; the Scala parser reads the package's key names."""
    cases, results = py_case_results
    expect = cases[name][2]
    result = results[name]
    text = exception_text(result["exception"])
    if expect == "reads":
        assert not result["exception"], text
        assert result.get("active_at_end") and result["rows"] > 0, result
    else:
        for part in expect:
            assert part in text, "%r not in:\n%s" % (part, text)


def test_pyspark_stateful_query_hdfs(scenario):
    """UC-56: a stateful aggregation with the default (HDFS) state store."""
    sc = scenario("py_stateful_hdfs")

    def final_counts(run):
        latest = {}
        for _, rows in sorted(run.records_by_batch().items()):
            for r in rows:
                latest[r["valBoolean"]] = r["count"]
        return latest

    _upload(sc, [0, 1])
    run = sc.job("run", sc.py_options("csv"), aggregate_by="valBoolean", pyspark=True).start()
    run.wait_started()
    wait_until(lambda: final_counts(run) == {True: 4, False: 2}, 300, 10, "counts after 2 files")
    _upload(sc, [2], wait=False)
    wait_until(lambda: final_counts(run) == {True: 6, False: 3}, 300, 10, "counts after 3 files")
    assert run.stop()["state"] == "stopped"
    assert final_counts(run) == {True: 6, False: 3}
    sc.assert_drained()
