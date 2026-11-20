"""Deterministic fixture files and the rows the job must emit for them.

Rows are (valString, valBoolean, valInt). File n, row r -> ("f{n}-r{r}", r % 2 == 0, n * 100 + r).
parquet and orc need pyarrow, imported only when those formats are built.
"""
from __future__ import annotations

import io
import json
from typing import List, Tuple

Row = Tuple[str, bool, int]

SCHEMA_DDL = "valString STRING, valBoolean BOOLEAN, valInt INT"
TEXT_SCHEMA_DDL = "value STRING"
ROWS_PER_FILE = 3


def rows_for(file_no: int, n: int = ROWS_PER_FILE) -> List[Row]:
    return [("f%d-r%d" % (file_no, r), r % 2 == 0, file_no * 100 + r) for r in range(n)]


def file_bytes(fmt: str, rows: List[Row], header: bool = False) -> bytes:
    """Encode rows in the given format, matching SCHEMA_DDL (or TEXT_SCHEMA_DDL for text)."""
    if fmt == "csv":
        lines = ["valString,valBoolean,valInt"] if header else []
        lines += ["%s,%s,%d" % (s, str(b).lower(), i) for s, b, i in rows]
        return ("\n".join(lines) + "\n").encode()
    if fmt == "json":
        return "".join(json.dumps({"valString": s, "valBoolean": b, "valInt": i}) + "\n"
                       for s, b, i in rows).encode()
    if fmt == "text":
        return "".join(s + "\n" for s, _, _ in rows).encode()
    if fmt in ("parquet", "orc"):
        import pyarrow as pa  # noqa: PLC0415 - optional, only for binary formats

        table = pa.table({
            "valString": pa.array([r[0] for r in rows], pa.string()),
            "valBoolean": pa.array([r[1] for r in rows], pa.bool_()),
            "valInt": pa.array([r[2] for r in rows], pa.int32()),
        })
        buf = io.BytesIO()
        if fmt == "parquet":
            import pyarrow.parquet as pq  # noqa: PLC0415

            pq.write_table(table, buf)
        else:
            import pyarrow.orc as orc  # noqa: PLC0415

            orc.write_table(table, buf)
        return buf.getvalue()
    raise ValueError("unsupported fixture format %r" % fmt)


def expected(fmt: str, rows: List[Row]) -> List[tuple]:
    """What the job's JSON output holds for these rows, as comparable tuples."""
    if fmt == "text":
        return [(s,) for s, _, _ in rows]
    return list(rows)


def output_tuple(fmt: str, record: dict, extra: Tuple[str, ...] = ()) -> tuple:
    """Turn one output JSON record into the tuple form of expected()."""
    cols = ("value",) if fmt == "text" else ("valString", "valBoolean", "valInt")
    return tuple(record.get(c) for c in cols + extra)
