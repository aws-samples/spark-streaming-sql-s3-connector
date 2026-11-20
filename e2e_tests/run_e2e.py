#!/usr/bin/env python3
r"""One command for the whole e2e run: terraform apply -> pytest per Spark version -> destroy.

    python e2e_tests/run_e2e.py --connector-jar target/spark-streaming-sql-s3-connector-0.0.3-spark3.5.2.jar \
        --release-label spark35=emr-7.5.0 [--suite all|core|pyspark] [-- <pytest args>]

    # Several Spark versions: one jar per tag (Spark 4 needs a Scala 2.13 build)
    python e2e_tests/run_e2e.py --release-label spark35=emr-7.5.0 --release-label spark40=emr-spark-8.0.0 \
        --connector-jar spark35=target/spark-streaming-sql-s3-connector-0.0.3-spark3.5.2.jar --connector-jar spark40=target/spark-streaming-sql-s3-connector-0.0.3-spark4.0.2.jar

This CREATES BILLABLE AWS RESOURCES in the account of your current AWS credentials and
destroys them at the end, also when tests fail. See e2e_tests/README.md for run time and cost.
Run one at a time per account and region: resource names are fixed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

HERE = Path(__file__).resolve().parent
TF_DIR = HERE.parent / "terraform"
VARS_FILE = TF_DIR / "e2e.auto.tfvars.json"

sys.path.insert(0, str(HERE))
from scenarios import SUITES  # noqa: E402


def parse_labels(values: List[str]) -> Dict[str, str]:
    """["spark35=emr-7.5.0", ...] -> {"spark35": "emr-7.5.0", ...}"""
    labels = {}
    for v in values:
        tag, sep, label = v.partition("=")
        if not sep or not tag or not label:
            raise argparse.ArgumentTypeError("--release-label wants TAG=LABEL, got %r" % v)
        labels[tag] = label
    return labels


def tf_vars(args: argparse.Namespace) -> Dict:
    remote = {"enabled": bool(args.remote_region or args.remote_assume_role_arn)}
    if args.remote_region:
        remote["region"] = args.remote_region
    if args.remote_assume_role_arn:
        remote["assume_role_arn"] = args.remote_assume_role_arn
    return {"region": args.region, "release_labels": parse_labels(args.release_label),
            "scenarios": SUITES[getattr(args, "suite", "all")][0], "remote": remote}


def suite_marker_args(suite: str, pytest_args: List[str]) -> List[str]:
    """pytest_args with the suite's marker ANDed into the user's -m (pytest keeps only the last -m)."""
    exprs = [SUITES[suite][1]] if SUITES[suite][1] else []
    rest: List[str] = []
    it = iter(pytest_args)
    for a in it:
        if a == "-m":
            exprs.append(next(it, ""))
        elif a.startswith("-m") and len(a) > 2:
            exprs.append(a[2:])
        else:
            rest.append(a)
    exprs = [e for e in exprs if e.strip()]
    return (["-m", " and ".join("(%s)" % e for e in exprs)] if exprs else []) + rest


def tf(*cmd: str) -> None:
    print("+ terraform " + " ".join(cmd), flush=True)
    subprocess.run(["terraform", "-chdir=%s" % TF_DIR, *cmd], check=True)


def stop_applications() -> None:
    """Cancel leftover job runs and stop each application; Terraform cannot delete a started one."""
    import boto3  # noqa: PLC0415 - only needed at teardown

    out = subprocess.run(["terraform", "-chdir=%s" % TF_DIR, "output", "-json"],
                         capture_output=True, text=True, check=False)
    if out.returncode != 0 or not out.stdout.strip():
        return
    outputs = json.loads(out.stdout)
    if "applications" not in outputs:
        return
    emr = boto3.client("emr-serverless", region_name=outputs["region"]["value"])
    for app in outputs["applications"]["value"].values():
        try:
            _stop_application(emr, app["id"])
        except Exception as exc:  # noqa: BLE001 - keep going with the other applications
            print("WARNING: stopping application %s failed: %r" % (app["id"], exc), file=sys.stderr)


def _stop_application(emr, app_id: str) -> None:
    for page in emr.get_paginator("list_job_runs").paginate(
            applicationId=app_id, states=["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING", "QUEUED"]):
        for run in page["jobRuns"]:
            try:
                emr.cancel_job_run(applicationId=app_id, jobRunId=run["id"])
            except emr.exceptions.ValidationException:
                pass  # finished between list and cancel
    deadline = time.monotonic() + 600
    while True:
        state = emr.get_application(applicationId=app_id)["application"]["state"]
        if state in ("CREATED", "STOPPED", "TERMINATED"):
            return
        if time.monotonic() > deadline:
            print("WARNING: application %s still %s after 10 min; destroy may fail" % (app_id, state),
                  file=sys.stderr)
            return
        if state == "STARTED":
            try:
                emr.stop_application(applicationId=app_id)
            except emr.exceptions.ValidationException:
                pass  # job runs still cancelling; retry
        time.sleep(15)


def parse_jars(values: List[str], tags: List[str]) -> Dict[str, Path]:
    """--connector-jar PATH (all tags) or TAG=PATH per tag -> {tag: jar}.

    Spark 4 (emr-spark-8.x) needs a Scala 2.13 build, so different tags usually need different jars.
    """
    default, per_tag = None, {}
    for v in values:
        tag, sep, path = v.partition("=")
        if sep and tag in tags:
            per_tag[tag] = Path(path)
        else:
            default = Path(v)
    jars = {}
    for tag in tags:
        jar = per_tag.get(tag, default)
        if jar is None:
            raise argparse.ArgumentTypeError("no --connector-jar for release tag %r" % tag)
        jars[tag] = jar.resolve()
    return jars


def teardown() -> None:
    """Destroy everything; never let a stop error skip destroy. Retry destroy once."""
    for attempt in (1, 2):
        try:
            stop_applications()
        except Exception as exc:  # noqa: BLE001 - destroy must still run
            print("WARNING: stopping applications failed: %r" % exc, file=sys.stderr)
        try:
            tf("destroy", "-input=false", "-auto-approve")
            VARS_FILE.unlink(missing_ok=True)
            return
        except subprocess.CalledProcessError:
            if attempt == 2:
                print("RESOURCES LEFT BEHIND (NAT gateway and EIP bill hourly). Run: terraform -chdir=%s "
                      "destroy -auto-approve" % TF_DIR, file=sys.stderr)
                raise
            time.sleep(60)


def main(argv: List[str]) -> int:
    pytest_args: List[str] = []
    if "--" in argv:
        i = argv.index("--")
        argv, pytest_args = argv[:i], argv[i + 1:]
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--connector-jar", action="append", required=True,
                   help="connector jar with dependencies: PATH for every tag, or TAG=PATH (repeat)")
    p.add_argument("--release-label", action="append", default=[],
                   help="TAG=EMR_RELEASE_LABEL, repeat for several Spark versions (default spark35=emr-7.5.0)")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--suite", choices=sorted(SUITES), default="all",
                   help="all (default), core (raw option keys, no PySpark package) or pyspark "
                        "(through the python/ package); only the suite's queues are created")
    p.add_argument("--remote-region", help="also test a bucket and queue in this region")
    p.add_argument("--remote-assume-role-arn", help="role in a second account for the remote source")
    args = p.parse_args(argv)
    args.release_label = args.release_label or ["spark35=emr-7.5.0"]
    try:
        tags = list(parse_labels(args.release_label))
        jars = parse_jars(args.connector_jar, tags)
    except argparse.ArgumentTypeError as exc:
        p.error(str(exc))
    for jar in jars.values():
        if not jar.is_file():
            p.error("connector jar not found: %s" % jar)

    VARS_FILE.write_text(json.dumps(tf_vars(args), indent=2))
    worst = 0
    try:
        tf("init", "-input=false")
        tf("apply", "-input=false", "-auto-approve")
        for tag in tags:
            cmd = [sys.executable, "-m", "pytest", str(HERE), "--release-tag", tag,
                   "--connector-jar", str(jars[tag]), "--junitxml", str(HERE / ("junit-%s.xml" % tag)),
                   *suite_marker_args(args.suite, pytest_args)]
            print("+ " + " ".join(cmd), flush=True)
            worst = max(worst, subprocess.run(cmd, cwd=HERE, check=False).returncode)
    finally:
        teardown()
    return worst


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
