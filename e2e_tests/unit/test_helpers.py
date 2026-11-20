"""Offline tests of the pure helpers. Run: pytest e2e_tests/unit"""
import io
import json
import zipfile
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import pytest

from harness import checkpoint, fixtures
from harness.events import (PREFIX, base_options, exception_text, s3_event_body, s3_event_time,
                            sqs_key, wait_until)
from run_e2e import parse_labels, tf_vars
from scenarios import SCENARIOS
from test_failures import CASE_NAMES, build_cases


def test_sqs_key_forms():
    assert sqs_key("double_dot", "maxRetries") == "spark.s3conn..sqs.maxRetries"
    assert sqs_key("single_dot", "maxRetries") == "spark.s3conn.sqs.maxRetries"


def test_base_options_have_required_keys():
    opts = base_options("https://q", "eu-west-1", "csv", sqs_form="single_dot")
    assert opts[PREFIX + "queueUrl"] == "https://q"
    assert opts[PREFIX + "queueRegion"] == "eu-west-1"
    assert opts[PREFIX + "fileFormat"] == "csv"
    assert "spark.s3conn.sqs.longPollingWaitTimeSeconds" in opts


def test_event_time_matches_connector_format():
    ts = datetime(2026, 11, 18, 7, 5, 3, 45000, tzinfo=timezone.utc)
    # Utils.convertTimestampToMills parses yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    assert s3_event_time(ts) == "2026-11-18T07:05:03.045Z"


def test_event_body_key_round_trips_like_urldecoder():
    key = "input/fmt_csv/a b+c=1.csv"
    body = json.loads(s3_event_body("bkt", key, datetime.now(timezone.utc)))
    rec = body["Records"][0]
    assert rec["eventName"] == "ObjectCreated:Put"
    assert rec["s3"]["object"]["key"] == "input/fmt_csv/a+b%2Bc%3D1.csv"
    assert unquote_plus(rec["s3"]["object"]["key"]) == key  # what java URLDecoder does


def test_batch_ids_ignore_crc_and_temp_files():
    keys = ["c/offsets/0", "c/offsets/1", "c/offsets/.1.crc", "c/offsets/10", "c/offsets/.tmp-2"]
    assert checkpoint.batch_ids(keys) == [0, 1, 10]


def test_rocksdb_versions():
    keys = ["c/sources/0/2.zip", "c/sources/0/10.zip", "c/sources/0/SSTs/0001-abc.sst"]
    assert checkpoint.rocksdb_versions(keys) == [2, 10]


def test_source_log_offset():
    text = 'v1\n{"batchWatermarkMs":0,"batchTimestampMs":1,"conf":{}}\n{"logOffset":7}\n'
    assert checkpoint.source_log_offset(text) == 7
    with pytest.raises(ValueError):
        checkpoint.source_log_offset("garbage")


def test_rocksdb_zip_metadata():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("metadata", 'v1\n{"sstFiles":[],"numKeys":12}')
    assert checkpoint.rocksdb_zip_metadata(buf.getvalue())["numKeys"] == 12


def test_fixture_rows_are_deterministic():
    assert fixtures.rows_for(2) == [("f2-r0", True, 200), ("f2-r1", False, 201), ("f2-r2", True, 202)]


def test_csv_and_json_and_text_bytes():
    rows = fixtures.rows_for(0, 2)
    assert fixtures.file_bytes("csv", rows) == b"f0-r0,true,0\nf0-r1,false,1\n"
    assert fixtures.file_bytes("csv", rows, header=True).startswith(b"valString,valBoolean,valInt\n")
    assert [json.loads(x) for x in fixtures.file_bytes("json", rows).splitlines()][1] == \
        {"valString": "f0-r1", "valBoolean": False, "valInt": 1}
    assert fixtures.file_bytes("text", rows) == b"f0-r0\nf0-r1\n"
    assert fixtures.expected("text", rows) == [("f0-r0",), ("f0-r1",)]
    assert fixtures.output_tuple("csv", {"valString": "a", "valBoolean": True, "valInt": 1, "p": "x"},
                                 ("p",)) == ("a", True, 1, "x")


@pytest.mark.parametrize("fmt", ["parquet", "orc"])
def test_binary_formats_round_trip(fmt):
    pytest.importorskip("pyarrow")
    import pyarrow.orc as orc
    import pyarrow.parquet as pq

    rows = fixtures.rows_for(1)
    data = fixtures.file_bytes(fmt, rows)
    table = (pq.read_table if fmt == "parquet" else orc.read_table)(io.BytesIO(data))
    assert [tuple(r.values()) for r in table.to_pylist()] == rows
    assert str(table.schema.field("valInt").type) == "int32"


def test_unknown_format_rejected():
    with pytest.raises(ValueError):
        fixtures.file_bytes("avro", [])


def test_wait_until_returns_value_and_times_out():
    calls = iter([None, 0, "ok"])
    assert wait_until(lambda: next(calls), 5, 0) == "ok"
    with pytest.raises(TimeoutError):
        wait_until(lambda: None, 0, 0)


def test_exception_text():
    assert exception_text([{"class": "A", "message": "x"}, {"class": "B", "message": "y"}]) == "A: x\nB: y"
    assert exception_text(None) == ""


def test_failure_cases_cover_both_sqs_key_forms():
    for name in ["long_poll_out_of_range", "max_retries_zero", "max_concurrency_zero",
                 "visibility_zero", "keep_not_boolean"]:
        assert name + "_double_dot" in CASE_NAMES and name + "_single_dot" in CASE_NAMES
    cases = build_cases("https://q/a", "us-west-2", "https://q/b", "https://q/c")
    assert cases["wrong_queue_region"][0][PREFIX + "queueRegion"] == "us-east-2"
    assert cases["wrong_queue_region"][0][PREFIX + "queueUrl"] == "https://q/c"
    assert cases["wrong_queue_region"][2] == "reads_nothing"
    assert cases["reprocess_start_only"][0][PREFIX + "queueUrl"] == "https://q/b"
    assert cases["reprocess_start_only"][2] == "reads"
    assert cases["max_retries_zero_single_dot"][0]["spark.s3conn.sqs.maxRetries"] == "0"
    assert PREFIX + "fileFormat" not in cases["no_file_format"][0]
    raising = [n for n, (_, _, e) in cases.items() if isinstance(e, list)]
    assert len(raising) == len(cases) - 2
    for name, (opts, _, _) in cases.items():  # each key-form case uses only its own spelling
        if name.endswith("_single_dot"):
            assert not [k for k in opts if k.startswith("spark.s3conn..sqs.")], name
        if name.endswith("_double_dot"):
            assert not [k for k in opts if k.startswith("spark.s3conn.sqs.")], name


def test_parse_labels():
    assert parse_labels(["a=emr-7.5.0", "b=emr-spark-8.0.0"]) == {"a": "emr-7.5.0", "b": "emr-spark-8.0.0"}
    with pytest.raises(Exception):
        parse_labels(["nolabel"])


def test_tf_vars_remote_and_scenarios():
    class A:
        region, release_label = "us-east-1", ["t=emr-7.5.0"]
        remote_region, remote_assume_role_arn = "eu-west-1", None
    v = tf_vars(A)
    assert v["remote"] == {"enabled": True, "region": "eu-west-1"}
    assert v["scenarios"] == SCENARIOS


def test_scenario_names_fit_terraform_validation():
    import re
    assert len(set(SCENARIOS)) == len(SCENARIOS)
    assert all(re.fullmatch(r"[a-z0-9_]{1,40}", s) for s in SCENARIOS)
    used = {"fmt_csv", "fmt_json", "fmt_parquet", "fmt_orc", "fmt_text", "csv_header", "malformed",
            "partitions", "mfpt_limit", "mfpt_unlimited", "glob", "file_age", "duplicates", "bad_messages",
            "keep_double_dot", "keep_single_dot", "reprocess", "recovery", "stateful_hdfs",
            "stateful_rocksdb", "failures", "reprocess_partial", "wrong_region", "special_keys",
            "space_key"}
    assert used == set(SCENARIOS)
    assert not {"fanout", "fanout_a", "fanout_b"} & set(SCENARIOS)  # reserved in variables.tf


def test_parse_jars_default_and_per_tag(tmp_path):
    from run_e2e import parse_jars
    a, b = tmp_path / "a.jar", tmp_path / "b.jar"
    assert parse_jars([str(a)], ["x", "y"]) == {"x": a, "y": a}
    assert parse_jars([str(a), "y=%s" % b], ["x", "y"]) == {"x": a, "y": b}
    with pytest.raises(Exception):
        parse_jars(["y=%s" % b], ["x", "y"])


def test_base_options_set_every_sqs_setting_in_one_form():
    for form in ("double_dot", "single_dot"):
        sqs_keys = [k for k in base_options("u", "r", "csv", sqs_form=form) if ".sqs." in k]
        assert sorted(k.rsplit(".", 1)[1] for k in sqs_keys) == [
            "longPollingWaitTimeSeconds", "maxConcurrency", "maxRetries", "visibilityTimeoutSeconds"]
        other = "single_dot" if form == "double_dot" else "double_dot"
        assert not [k for k in sqs_keys if k.startswith(sqs_key(other, ""))]
