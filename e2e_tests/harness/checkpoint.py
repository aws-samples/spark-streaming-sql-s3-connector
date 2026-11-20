"""Pure parsers for Spark checkpoint files and the connector's RocksDB metadata log.

Layout under <checkpoint>/ (Spark): offsets/<batchId>, commits/<batchId>.
Layout under <checkpoint>/sources/0/ (connector RocksDB, RocksDBFileManager):
<version>.zip (contains `metadata` = "v1\\n{json with numKeys}") and SSTs/*.sst.
"""
from __future__ import annotations

import io
import json
import re
import zipfile
from typing import Dict, Iterable, List, Optional

_BATCH_FILE = re.compile(r"/(\d+)$")
_ZIP_FILE = re.compile(r"/(\d+)\.zip$")


def batch_ids(keys: Iterable[str]) -> List[int]:
    """Sorted batch ids from keys like .../offsets/3 (ignores .crc and temp files)."""
    return sorted(int(m.group(1)) for k in keys for m in [_BATCH_FILE.search(k)] if m)


def rocksdb_versions(keys: Iterable[str]) -> List[int]:
    """Sorted RocksDB versions from keys like .../sources/0/7.zip."""
    return sorted(int(m.group(1)) for k in keys for m in [_ZIP_FILE.search(k)] if m)


def source_log_offset(offset_file_text: str) -> Optional[int]:
    """logOffset of source 0 from an offsets/<n> file.

    Format: line 1 "v1", line 2 batch metadata JSON, then one line per source.
    The connector writes FileStreamSourceOffset: {"logOffset":N}.
    """
    lines = [ln for ln in offset_file_text.splitlines() if ln.strip()]
    if len(lines) < 3 or not lines[0].startswith("v"):
        raise ValueError("not a Spark offsets file: %r" % offset_file_text[:80])
    src = lines[2]
    if src == "-":
        return None
    return int(json.loads(src)["logOffset"])


def rocksdb_zip_metadata(zip_bytes: bytes) -> Dict:
    """Parse the `metadata` entry of a RocksDB checkpoint <version>.zip."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        text = zf.read("metadata").decode("utf-8")
    header, _, body = text.partition("\n")
    if header != "v1":
        raise ValueError("unknown RocksDB metadata version %r" % header)
    return json.loads(body)
