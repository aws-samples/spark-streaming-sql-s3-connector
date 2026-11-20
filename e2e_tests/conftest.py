"""pytest wiring: Terraform outputs -> Stack, artifact upload, per-test Scenario."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from harness.aws import Stack  # noqa: E402
from harness.scenario import Scenario  # noqa: E402


def pytest_addoption(parser):
    g = parser.getgroup("s3-connector e2e")
    g.addoption("--tf-dir", default=str(HERE.parent / "terraform"),
                help="Terraform directory with an applied stack (default: ../terraform)")
    g.addoption("--release-tag", default=None,
                help="Key of var.release_labels to run on (default: the only one, if there is one)")
    g.addoption("--connector-jar", default=os.environ.get("CONNECTOR_JAR"),
                help="Local path of the connector jar (with dependencies) under test")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--connector-jar"):
        return
    skip = pytest.mark.skip(reason="e2e test: pass --connector-jar (and an applied --tf-dir)")
    for item in items:
        if item.get_closest_marker("e2e"):
            item.add_marker(skip)


@pytest.fixture(scope="session")
def tf_outputs(pytestconfig):
    out = subprocess.run(["terraform", "-chdir=" + pytestconfig.getoption("--tf-dir"), "output", "-json"],
                         check=True, capture_output=True, text=True, timeout=120).stdout
    outputs = {k: v["value"] for k, v in json.loads(out).items()}
    if "bucket" not in outputs:
        pytest.exit("Terraform stack is not applied in %s" % pytestconfig.getoption("--tf-dir"), 2)
    return outputs


@pytest.fixture(scope="session")
def stack(tf_outputs, pytestconfig):
    tag = pytestconfig.getoption("--release-tag")
    if tag is None:
        apps = sorted(tf_outputs["applications"])
        if len(apps) != 1:
            pytest.exit("several applications %s: pass --release-tag" % apps, 2)
        tag = apps[0]
    return Stack(tf_outputs, tag)


@pytest.fixture(scope="session")
def session_id(stack):
    return "%s-%s" % (time.strftime("%Y%m%d%H%M%S"), stack.release_tag)


@pytest.fixture(scope="session")
def artifacts(stack, session_id, pytestconfig):
    jar = Path(pytestconfig.getoption("--connector-jar"))
    if not jar.is_file():
        pytest.exit("connector jar not found: %s" % jar, 2)
    prefix = "artifacts/%s/" % session_id
    stack.put(prefix + "connector.jar", jar.read_bytes())
    stack.put(prefix + "stream_job.py", (HERE / "job" / "stream_job.py").read_bytes())
    return {"jar": stack.s3_uri(prefix + "connector.jar"), "job": stack.s3_uri(prefix + "stream_job.py")}


@pytest.fixture
def scenario(stack, artifacts, session_id):
    """scenario("name") -> a reset Scenario; any job still running at the end is cancelled."""
    made = []

    def make(name):
        s = Scenario(stack, artifacts, name, session_id).reset()
        made.append(s)
        return s

    yield make
    for s in made:
        s.cleanup()
