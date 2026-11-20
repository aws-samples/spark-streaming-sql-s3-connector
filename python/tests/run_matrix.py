"""Run the tests once per PySpark version, each in its own uv venv.

Usage (from python/):
    python tests/run_matrix.py --work .work --jar 3.5=spark-streaming-sql-s3-connector-0.0.3-spark3.5.8.jar \
        --jar 4.1=spark-streaming-sql-s3-connector-0.0.3-spark4.1.3.jar
    python tests/run_matrix.py ... --only 3.5.8

Builds the wheel once, installs it with the pinned pyspark into each venv and
writes one pytest log per version to <work>/logs/. Needs uv and the JDKs below.
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

JAVA = {
    11: "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
    17: "/usr/lib/jvm/java-17-amazon-corretto.x86_64",
}

# pyspark version, Python, Java, platforms using that Spark version.
MATRIX = [
    ("3.2.1", "3.8", 11, "EMR 6.7"),
    ("3.3.0", "3.10", 11, "Glue 4.0, EMR 6.8-6.9"),
    ("3.4.1", "3.8", 11, "EMR 6.13-6.15"),
    ("3.5.4", "3.11", 11, "Glue 5.0, EMR 7.8"),
    ("3.5.6", "3.11", 11, "Glue 5.1, EMR 7.11-7.13"),
    ("3.5.8", "3.9", 11, "EMR 7.14"),
    ("4.0.2", "3.11", 17, "EMR emr-spark-8.0.0"),
    ("4.1.1", "3.13", 17, "Glue 6.0, EMR emr-spark-8.1.0"),
    ("4.1.3", "3.13", 17, "EMR emr-spark-8.2.0"),
]


def run(cmd, **kw):
    print("+", " ".join(map(str, cmd)), flush=True)
    return subprocess.run([str(x) for x in cmd], check=True, **kw)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", required=True, type=Path)
    ap.add_argument("--jar", action="append", default=[],
                    help="LINE=PATH, LINE is a Spark line (3.5) or major (3); e.g. 3.5=/path/spark-streaming-sql-s3-connector-0.0.3-spark3.5.8.jar")
    ap.add_argument("--only", action="append", default=[], help="pyspark version(s) to run")
    ap.add_argument("--uv", default=shutil.which("uv") or "uv")
    args = ap.parse_args()

    jars = {}
    for item in args.jar:
        major, _, path = item.partition("=")
        jars[major] = str(Path(path).resolve())

    here = Path(__file__).resolve().parent.parent
    work = args.work.resolve()
    (work / "logs").mkdir(parents=True, exist_ok=True)
    dist = work / "dist"
    for old in dist.glob("*.whl"):
        old.unlink()
    run([args.uv, "build", "--wheel", "--out-dir", dist, here])
    wheel = next(dist.glob("*.whl"))

    failed = []
    for version, python, java, platforms in MATRIX:
        if args.only and version not in args.only:
            continue
        if not Path(JAVA[java], "bin", "java").exists():
            sys.exit(f"JDK {java} not found at {JAVA[java]}; edit JAVA in {__file__}")
        venv = work / "venvs" / f"pyspark-{version}"
        run([args.uv, "venv", "--clear", "--python", python, venv])
        py = venv / "bin" / "python"
        run([args.uv, "pip", "install", "--python", py, f"pyspark=={version}", "pytest", wheel])

        env = dict(os.environ)
        env.pop("CONNECTOR_JAR", None)
        env.update(JAVA_HOME=JAVA[java], PATH=f"{JAVA[java]}/bin:{env['PATH']}",
                   EXPECT_PYSPARK=version, PYSPARK_PYTHON=str(py))
        major, minor = version.split(".")[:2]
        jar = jars.get(f"{major}.{minor}") or jars.get(major)
        if jar:
            env["CONNECTOR_JAR"] = jar
        log = work / "logs" / f"pytest-pyspark-{version}.log"
        with open(log, "w") as f:
            f.write(f"# pyspark {version}, Python {python}, Java {java} ({platforms})\n")
            f.write(f"# CONNECTOR_JAR={jar}\n")
            f.flush()
            try:
                rc = subprocess.run(
                    [str(py), "-m", "pytest", "-v", "-rs", "-p", "no:cacheprovider", "tests"],
                    cwd=here, env=env, stdout=f, stderr=subprocess.STDOUT, timeout=1800,
                ).returncode
            except subprocess.TimeoutExpired:
                f.write("\nTIMEOUT after 1800 s\n")
                rc = 124
        tail = log.read_text().strip().splitlines()[-1]
        print(f"pyspark {version}: rc={rc} {tail} -> {log}", flush=True)
        if rc:
            failed.append(version)

    print("FAILED:" if failed else "ALL PASSED", " ".join(failed))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
