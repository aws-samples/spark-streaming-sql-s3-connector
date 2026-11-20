"""UC-01..UC-10: file formats, format options, malformed rows, special keys."""
from collections import Counter

import pytest

from harness.events import PREFIX
from harness.fixtures import SCHEMA_DDL, TEXT_SCHEMA_DDL, expected, file_bytes, output_tuple, rows_for

pytestmark = pytest.mark.e2e

EXT = {"csv": "csv", "json": "json", "parquet": "parquet", "orc": "orc", "text": "txt"}


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet", "orc", "text"])
def test_file_format(scenario, fmt):
    sc = scenario("fmt_" + fmt)
    names = ["f0.%s" % EXT[fmt], "f1.%s" % EXT[fmt]]
    want = []
    for n, name in enumerate(names):
        rows = rows_for(n)
        sc.upload(name, file_bytes(fmt, rows))
        want += expected(fmt, rows)
    sc.stack.wait_visible(sc.queue_url, len(names))

    extra = {PREFIX + "queueType": "SQS"} if fmt == "json" else {}  # UC-10
    run = sc.run_until_rows("run", sc.options(fmt, **extra), len(want),
                            schema=TEXT_SCHEMA_DDL if fmt == "text" else SCHEMA_DDL)

    got = [output_tuple(fmt, r) for r in run.records()]
    assert Counter(got) == Counter(want)
    # input_file_name() may URI-encode the key, so compare counts, not names.
    files = {f for m in run.manifests() for f in m["files"]}
    assert len(files) == len(names), files
    sc.assert_drained()

    state = sc.stack.checkpoint_state(sc.checkpoint_prefix)
    last_batch = run.manifests()[-1]["batch_id"]
    assert state["commits"] and state["commits"][-1] == last_batch, state
    assert state["log_offset"] == last_batch, state
    assert state["rocksdb_versions"], state
    assert state["num_keys"] >= len(names), state


@pytest.mark.parametrize("scenario_name, key", [
    ("special_keys", "a+b=1,c.csv"),  # S3 sends %2B, %3D, %2C; the connector URL-decodes them
    ("space_key", "with space.csv"),  # S3 sends '+'; fixed in getBatch (was URISyntaxException)
])
def test_special_characters_in_key(scenario, scenario_name, key):
    sc = scenario(scenario_name)
    rows = rows_for(0)
    sc.upload(key, file_bytes("csv", rows))
    sc.stack.wait_visible(sc.queue_url, 1)

    run = sc.run_until_rows("run", sc.options("csv"), len(rows))
    assert Counter(output_tuple("csv", r) for r in run.records()) == Counter(rows)
    sc.assert_drained()


def test_csv_header_option(scenario):
    sc = scenario("csv_header")
    rows = rows_for(0)
    sc.upload("h0.csv", file_bytes("csv", rows, header=True))
    sc.stack.wait_visible(sc.queue_url, 1)

    run = sc.run_until_rows("run", sc.options("csv", header="true"), len(rows), quiet_s=10)

    got = [output_tuple("csv", r) for r in run.records()]
    assert Counter(got) == Counter(rows)  # the header line is not a data row
    sc.assert_drained()


def test_malformed_rows_dropped(scenario):
    sc = scenario("malformed")
    good = rows_for(0)
    body = file_bytes("csv", good) + b"bad,notabool,notanint\n"
    sc.upload("m0.csv", body)
    sc.stack.wait_visible(sc.queue_url, 1)

    run = sc.run_until_rows("run", sc.options("csv", mode="DROPMALFORMED"), len(good), quiet_s=10)

    got = [output_tuple("csv", r) for r in run.records()]
    assert Counter(got) == Counter(good)
    sc.assert_drained()
