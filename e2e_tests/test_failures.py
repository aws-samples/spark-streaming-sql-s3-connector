"""UC-09, UC-30, UC-36, UC-37, UC-43..UC-51: configuration errors. One EMR job runs every case."""
import pytest

from harness.events import PREFIX, SQS_PREFIXES, base_options, exception_text, sqs_key
from harness.fixtures import SCHEMA_DDL, file_bytes, rows_for
from harness.job import JobRun
from harness.scenario import Scenario

pytestmark = pytest.mark.e2e

RAISE_SECONDS = 90  # the source is built in the first micro-batch; errors show within seconds
RUNS_SECONDS = 40   # non-raising cases must still be up after this long


def build_cases(queue_url, region, partial_url, wrong_region_url):
    """name -> (options, schema, expectation).

    expectation: list of substrings of the exception chain, "reads" (no error, rows > 0) or
    "reads_nothing" (no error, no rows). The two non-raising cases each get their own queue
    with one pending file, so "reads" and "reads nothing" are both observable.
    """
    base = base_options(queue_url, region, "csv")
    other_region = "us-west-2" if region != "us-west-2" else "us-east-2"

    def without(key):
        return {k: v for k, v in base.items() if k != key}

    cases = {
        "no_schema": (base, None, ["S3-connector source doesn't support empty schema"]),
        "no_file_format": (without(PREFIX + "fileFormat"), SCHEMA_DDL,
                           ["IllegalArgumentException", PREFIX + "fileFormat"]),
        "no_queue_url": (without(PREFIX + "queueUrl"), SCHEMA_DDL,
                         ["IllegalArgumentException", PREFIX + "queueUrl is not specified"]),
        "no_queue_region": (without(PREFIX + "queueRegion"), SCHEMA_DDL,
                            ["IllegalArgumentException", PREFIX + "queueRegion is not specified"]),
        "bad_queue_type": (dict(base, **{PREFIX + "queueType": "Kafka"}), SCHEMA_DDL,
                           ["S3ConnectorUnsupportedQueueTypeException"]),
        "fetch_wait_zero": (dict(base, **{PREFIX + "queueFetchWaitTimeoutSeconds": "0"}), SCHEMA_DDL,
                            ["IllegalArgumentException", "Invalid value '0'"]),
        "reprocess_start_after_end": (dict(base, **{PREFIX + "reprocessStartBatchId": "5",
                                                    PREFIX + "reprocessEndBatchId": "2"}), SCHEMA_DDL,
                                      ["IllegalArgumentException", "reprocessStartBatchId must be less than"]),
        "reprocess_negative": (dict(base, **{PREFIX + "reprocessStartBatchId": "-1",
                                             PREFIX + "reprocessEndBatchId": "2"}), SCHEMA_DDL,
                               ["IllegalArgumentException", "must be zero or a positive integer"]),
        "reprocess_dry_run_not_boolean": (dict(base, **{PREFIX + "reprocessDryRun": "maybe"}), SCHEMA_DDL,
                                          ["IllegalArgumentException", "Invalid value 'maybe'"]),
        # UC-37: only a start id -> NoReprocess, so new messages are read.
        "reprocess_start_only": (dict(base_options(partial_url, region, "csv"),
                                      **{PREFIX + "reprocessStartBatchId": "0"}), SCHEMA_DDL, "reads"),
        # UC-30: receive errors are only logged; the stream stays up and reads nothing.
        # Holds for the SDK in pom.xml (2.19.x), which sends to the queueRegion endpoint. An SDK
        # that routes by the queue URL host instead would read the file and fail this case.
        "wrong_queue_region": (base_options(wrong_region_url, other_region, "csv"), SCHEMA_DDL,
                               "reads_nothing"),
    }
    for form in SQS_PREFIXES:
        form_base = base_options(queue_url, region, "csv", sqs_form=form)  # one spelling per case
        for name, key, value in [
            ("long_poll_out_of_range", "longPollingWaitTimeSeconds", "30"),
            ("max_retries_zero", "maxRetries", "0"),
            ("max_concurrency_zero", "maxConcurrency", "0"),
            ("visibility_zero", "visibilityTimeoutSeconds", "0"),
            ("keep_not_boolean", "keepMessageForConsumerError", "maybe"),
        ]:
            cases["%s_%s" % (name, form)] = (dict(form_base, **{sqs_key(form, key): value}), SCHEMA_DDL,
                                             ["IllegalArgumentException", "Invalid value '%s'" % value])
    return cases


def _param(name):
    marks = [pytest.mark.sqs_single_dot] if name.endswith("_single_dot") else []
    return pytest.param(name, marks=marks, id=name)


CASE_NAMES = sorted(build_cases("https://q/a", "us-east-1", "https://q/b", "https://q/c"))


@pytest.fixture(scope="module")
def case_results(stack, artifacts, session_id):
    sc = Scenario(stack, artifacts, "failures", session_id).reset()
    partial = Scenario(stack, artifacts, "reprocess_partial", session_id).reset()
    wrong = Scenario(stack, artifacts, "wrong_region", session_id).reset()
    for s in (partial, wrong):
        s.upload("f0.csv", file_bytes("csv", rows_for(0)))
        stack.wait_visible(s.queue_url, 1)
    cases = build_cases(sc.queue_url, stack.region, partial.queue_url, wrong.queue_url)
    config = {"mode": "cases", "cases": [
        {"name": n, "options": o, "schema": s,
         "seconds": RAISE_SECONDS if isinstance(e, list) else RUNS_SECONDS}
        for n, (o, s, e) in sorted(cases.items())]}
    run = JobRun(stack, artifacts, sc.run_prefix, "cases", config, timeout_minutes=60)
    try:
        status = run.start().wait_done(timeout_s=60 * 60)
    finally:
        run.cleanup()
    # The wrong-region stream must not have consumed its queue's message.
    return cases, status["cases"], stack.counts(wrong.queue_url)


@pytest.mark.parametrize("name", [_param(n) for n in CASE_NAMES])
def test_invalid_configuration(case_results, name):
    cases, results, wrong_region_left = case_results
    _, _, expect = cases[name]
    result = results[name]
    text = exception_text(result["exception"])
    if expect == "reads":
        assert not result["exception"], text
        assert result.get("active_at_end") and result["rows"] > 0, result
    elif expect == "reads_nothing":
        assert not result["exception"], text
        assert result.get("active_at_end") and result["rows"] == 0, result
        assert wrong_region_left >= 1, "message left the wrong-region queue"
    else:
        for part in expect:
            assert part in text, "%r not in:\n%s" % (part, text)
