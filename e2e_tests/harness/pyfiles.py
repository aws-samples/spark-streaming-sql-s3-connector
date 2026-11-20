"""The --py-files zip for job/pyspark_job.py: the PySpark helper package plus stream_job.py.

No AWS calls here, so this module is unit-tested offline (e2e_tests/unit).
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

E2E_DIR = Path(__file__).resolve().parent.parent
PACKAGE_DIR = E2E_DIR.parent / "python" / "src" / "spark_s3_connector"
STREAM_JOB = E2E_DIR / "job" / "stream_job.py"


def deps_zip(package_dir: Path = PACKAGE_DIR, stream_job: Path = STREAM_JOB) -> bytes:
    """Zip with spark_s3_connector/*.py and stream_job.py at its root (sys.path entries on EMR)."""
    # shortcut: flat package only; use rglob if the package gets subpackages.
    sources = sorted(package_dir.glob("*.py"))
    if not (package_dir / "__init__.py").is_file():
        raise FileNotFoundError("PySpark helper package not found in %s" % package_dir)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for src in sources:
            zf.write(src, "%s/%s" % (package_dir.name, src.name))
        zf.write(stream_job, stream_job.name)
    return buf.getvalue()
