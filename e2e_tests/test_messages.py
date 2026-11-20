"""UC-17..UC-26: duplicates, bad messages, keep-message-on-error (both key forms), SNS fan-out."""
import json
from collections import Counter
from datetime import datetime, timezone

import pytest

from harness.events import PREFIX, s3_event_body, s3_test_event_body, sqs_key, wait_until
from harness.fixtures import file_bytes, output_tuple, rows_for
from harness.job import wait_quiet

pytestmark = pytest.mark.e2e


def test_duplicate_files_and_messages(scenario):
    sc = scenario("duplicates")
    a, b, c = rows_for(0), rows_for(1), rows_for(2)
    sc.upload("a.csv", file_bytes("csv", a))
    key_b = sc.upload("b.csv", file_bytes("csv", b))
    sc.stack.wait_visible(sc.queue_url, 2)

    run = sc.job("run", sc.options("csv")).start()
    run.wait_started()
    run.wait_rows(len(a) + len(b))

    sc.upload("a.csv", file_bytes("csv", a))  # UC-18: same object again, new S3 event
    sc.stack.send(sc.queue_url, s3_event_body(sc.stack.bucket, key_b, datetime.now(timezone.utc)))  # UC-19
    sc.upload("c.csv", file_bytes("csv", c))  # sentinel: once c is read, the duplicates were seen too
    run.wait_rows(len(a) + len(b) + len(c))
    sc.assert_drained()
    # Messages are deleted when a batch is planned, before it runs: give a wrongly accepted
    # duplicate a few triggers to reach the output before stopping.
    wait_quiet(15)
    assert run.stop()["state"] == "stopped"

    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(a + b + c)


def test_non_create_and_malformed_messages(scenario):
    sc = scenario("bad_messages")
    now = datetime.now(timezone.utc)
    for body in [
        s3_test_event_body(sc.stack.bucket),
        s3_event_body(sc.stack.bucket, sc.key("gone.csv"), now, event_name="ObjectRemoved:Delete"),
        "this is not json",
        json.dumps({"Records": []}),
    ]:
        sc.stack.send(sc.queue_url, body)
    good = rows_for(0)
    sc.upload("good.csv", file_bytes("csv", good))
    sc.stack.wait_visible(sc.queue_url, 5)

    run = sc.run_until_rows("run", sc.options("csv"), len(good), quiet_s=15)

    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(good)
    sc.assert_drained()  # unparseable and non-create messages are deleted, none reach the DLQ


@pytest.mark.parametrize("form", [
    "double_dot",
    pytest.param("single_dot", marks=pytest.mark.sqs_single_dot),
])
def test_keep_message_on_error(scenario, form):
    sc = scenario("keep_" + form)
    good = rows_for(0)
    sc.upload("good.csv", file_bytes("csv", good))
    skip_key = sc.upload("skip.txt", file_bytes("csv", rows_for(1)))
    sc.stack.wait_visible(sc.queue_url, 2)

    # sqs_form makes every sqs.* key in the options use this one spelling only.
    opts = sc.options("csv", sqs_form=form, **{
        PREFIX + "pathGlobFilter": "*.csv",
        sqs_key(form, "keepMessageForConsumerError"): "true",
        sqs_key(form, "visibilityTimeoutSeconds"): "5",
    })
    run = sc.job("run", opts).start()
    run.wait_started()
    run.wait_rows(len(good))

    # The kept message comes back every 5 s; after max_receive_count receives SQS moves it to the DLQ.
    wait_until(lambda: sc.stack.counts(sc.dlq_url) >= 1, 240, 10,
               "kept message in the DLQ (spark.s3conn %s keys)" % form)
    assert run.stop()["state"] == "stopped"

    dlq = sc.stack.receive_all(sc.dlq_url)
    assert len(dlq) == 1 and skip_key in json.loads(dlq[0]["Body"])["Records"][0]["s3"]["object"]["key"], dlq
    # SQS approximate counts lag a redrive to the DLQ by up to a minute
    wait_until(lambda: sc.stack.counts(sc.queue_url) == 0, 90, 10, "main queue empty")
    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(good)


def test_sns_fanout(scenario):
    qa, qb = scenario("fanout_a"), scenario("fanout_b")
    rows = rows_for(0)
    qa.upload("x.csv", file_bytes("csv", rows))  # one object, both queues get the event via SNS
    qa.stack.wait_visible(qa.queue_url, 1)
    qb.stack.wait_visible(qb.queue_url, 1)

    runs = [q.job("run", q.options("csv")).start() for q in (qa, qb)]
    for run in runs:
        run.wait_started()
        run.wait_rows(len(rows))
        assert run.stop()["state"] == "stopped"
        assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(rows)
    qa.assert_drained()
    qb.assert_drained()
