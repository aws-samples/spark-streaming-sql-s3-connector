"""UC-11..UC-16, UC-22: partitions, maxFilesPerTrigger, pathGlobFilter, maxFileAge."""
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest

from harness.events import PREFIX, s3_event_body
from harness.fixtures import file_bytes, output_tuple, rows_for
from harness.job import wait_quiet

pytestmark = pytest.mark.e2e


def test_partition_columns(scenario):
    sc = scenario("partitions")
    want = []
    parts = [("A", "x"), ("A", "y"), ("B", "x")]
    for n, (p1, p2) in enumerate(parts):
        rows = rows_for(n)
        sc.upload("p1=%s/p2=%s/f%d.csv" % (p1, p2, n), file_bytes("csv", rows))
        want += [r + (p1, p2) for r in rows]
    sc.stack.wait_visible(sc.queue_url, len(parts))

    opts = sc.options("csv", **{PREFIX + "partitionColumns": "p1, p2", "basePath": sc.uri("")})
    run = sc.run_until_rows("run", opts, len(want),
                            schema="valString STRING, valBoolean BOOLEAN, valInt INT, p1 STRING, p2 STRING")

    got = [output_tuple("csv", r, ("p1", "p2")) for r in run.records()]
    assert Counter(got) == Counter(want)
    sc.assert_drained()


def _upload_files(sc, count):
    want = []
    for n in range(count):
        rows = rows_for(n)
        sc.upload("f%d.csv" % n, file_bytes("csv", rows))
        want += rows
    sc.stack.wait_visible(sc.queue_url, count)
    return want


def test_max_files_per_trigger_limit(scenario):
    sc = scenario("mfpt_limit")
    want = _upload_files(sc, 7)

    run = sc.run_until_rows("run", sc.options("csv", **{PREFIX + "maxFilesPerTrigger": "3"}), len(want))

    manifests = run.manifests()
    assert all(len(m["files"]) <= 3 for m in manifests), manifests
    assert len(manifests) >= 3, manifests
    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(want)
    sc.assert_drained()


def test_max_files_per_trigger_unlimited(scenario):
    sc = scenario("mfpt_unlimited")
    want = _upload_files(sc, 7)  # all 7 are queued before the job starts

    run = sc.run_until_rows("run", sc.options("csv", **{PREFIX + "maxFilesPerTrigger": "-1"}), len(want))

    # With a cap of 3 no batch could hold more; -1 removes the cap.
    assert max(len(m["files"]) for m in run.manifests()) > 3, run.manifests()
    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(want)
    sc.assert_drained()


def test_path_glob_filter(scenario):
    sc = scenario("glob")
    want = []
    for n in range(3):
        rows = rows_for(n)
        sc.upload("in%d.csv" % n, file_bytes("csv", rows))
        want += rows
    for n in range(3, 5):
        sc.upload("skip%d.txt" % n, file_bytes("csv", rows_for(n)))  # csv content, wrong name
    sc.stack.wait_visible(sc.queue_url, 5)

    run = sc.run_until_rows("run", sc.options("csv", **{PREFIX + "pathGlobFilter": "*.csv"}),
                            len(want), quiet_s=15)

    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(want)
    # UC-22: keepMessageForConsumerError defaults to false, so skipped files' messages are deleted.
    sc.assert_drained()


def test_max_file_age_and_late_files(scenario):
    sc = scenario("file_age")
    # Objects for synthetic events live outside the notified prefix, so S3 sends no real event.
    synthetic = "input/file_age_synthetic/"
    sc.stack.delete_prefix(synthetic)
    now = datetime.now(timezone.utc)

    first = rows_for(0)
    sc.upload("now.csv", file_bytes("csv", first))
    sc.stack.wait_visible(sc.queue_url, 1)

    run = sc.job("run", sc.options("csv", **{PREFIX + "maxFileAge": "1h"})).start()
    run.wait_started()
    run.wait_rows(len(first))  # latest file time is now, so files older than now-1h expire

    late, old = rows_for(1), rows_for(2)
    sc.stack.put(synthetic + "late.csv", file_bytes("csv", late))
    sc.stack.put(synthetic + "old.csv", file_bytes("csv", old))
    sc.stack.send(sc.queue_url, s3_event_body(sc.stack.bucket, synthetic + "late.csv", now - timedelta(minutes=30)))
    sc.stack.send(sc.queue_url, s3_event_body(sc.stack.bucket, synthetic + "old.csv", now - timedelta(hours=3)))

    run.wait_rows(len(first) + len(late))
    sc.assert_drained()  # the expired file's message is deleted, not left behind
    wait_quiet(15)  # deletion happens at planning time; let a wrongly accepted file reach the output
    status = run.stop()
    assert status["state"] == "stopped", status

    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(first + late)
