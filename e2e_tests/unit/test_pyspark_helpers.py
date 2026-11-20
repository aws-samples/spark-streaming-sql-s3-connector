"""Offline tests of the PySpark lane: --py-files zip, suites, read_stream() kwargs."""
import io
import json
import sys
import zipfile
from argparse import Namespace

import pytest

from harness.events import base_options, py_base_options
from harness.pyfiles import PACKAGE_DIR, deps_zip
from run_e2e import suite_marker_args, tf_vars
from scenarios import CORE_SCENARIOS, PYSPARK_SCENARIOS, SUITES
from test_pyspark import OPTION_CASES, build_option_cases

sys.path.insert(0, str(PACKAGE_DIR.parent))
from spark_s3_connector import read_stream, s3_connector_options  # noqa: E402


def test_deps_zip_has_package_and_stream_job():
    names = set(zipfile.ZipFile(io.BytesIO(deps_zip())).namelist())
    assert "stream_job.py" in names
    assert "spark_s3_connector/__init__.py" in names
    assert not [n for n in names if "__pycache__" in n or not n.endswith(".py")], names


def test_deps_zip_needs_the_package(tmp_path):
    with pytest.raises(FileNotFoundError):
        deps_zip(package_dir=tmp_path)


def test_suites_split_scenarios_and_markers():
    assert SUITES["all"][0] == CORE_SCENARIOS + PYSPARK_SCENARIOS
    assert not set(CORE_SCENARIOS) & set(PYSPARK_SCENARIOS)
    assert suite_marker_args("all", ["-v"]) == ["-v"]
    assert suite_marker_args("core", []) == ["-m", "(not pyspark)"]
    assert suite_marker_args("pyspark", ["-m", "not sqs_single_dot", "-k", "fmt"]) == [
        "-m", "(pyspark) and (not sqs_single_dot)", "-k", "fmt"]
    assert suite_marker_args("all", ["-mslow"]) == ["-m", "(slow)"]
    args = Namespace(region="us-east-2", release_label=["t=emr-7.5.0"], remote_region=None,
                     remote_assume_role_arn=None, suite="pyspark")
    assert tf_vars(args)["scenarios"] == PYSPARK_SCENARIOS


def test_py_base_options_match_raw_single_dot_options():
    """The package turns the harness kwargs into the same keys the core suite sets by hand."""
    got = s3_connector_options(**py_base_options("https://q", "us-east-2", "csv"))
    want = base_options("https://q", "us-east-2", "csv", sqs_form="single_dot")
    want["spark.s3conn.queueType"] = "SQS"  # the package always sets it
    assert got == want


def test_option_cases_raise_in_the_package_or_pass_it():
    cases = build_option_cases("https://q", "us-east-2")
    assert OPTION_CASES[0] == "reads"  # runs while its file is still pending
    for name, (kwargs, schema, expect) in cases.items():
        kwargs = json.loads(json.dumps(kwargs))  # as the job reads them from config.json
        if name.startswith("pkg_"):
            # read_stream raises before it touches spark, so None stands in for the session.
            with pytest.raises(Exception) as err:
                read_stream(None, schema, **kwargs)
            assert type(err.value).__name__ == expect[0], name
            assert expect[1] in str(err.value), (name, str(err.value))
        else:
            s3_connector_options(**kwargs)  # reaches Spark; the connector decides
