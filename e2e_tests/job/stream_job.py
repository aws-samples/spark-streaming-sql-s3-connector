"""Spark driver for the e2e tests. Runs on EMR Serverless with the connector jar on --jars.

Usage: stream_job.py s3://<bucket>/run/<scenario>/<run>/config.json

Modes (config["mode"]):
  stream  Read with the connector, write every micro-batch to <output>/batches/batch=<id>/
          as JSON and <output>/manifests/<id>.json, until <output>/STOP exists or
          max_seconds passes. Writes <output>/status.json at the end. With
          crash_after_batch=N the driver exits hard right after batch N's sink write.
  cases   Start one short query per case and record the exception chain (or none) and
          the row count of each in <output>/status.json.

The job always exits 0 after writing status.json; the harness asserts on that file.
Only Hadoop FileSystem (via the JVM) is used for S3, so no Python packages are needed.
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _fs(spark, path):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), p


def read_text(spark, path):
    fs, p = _fs(spark, path)
    stream = fs.open(p)
    try:
        jvm = spark.sparkContext._jvm
        return jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def write_text(spark, path, text):
    fs, p = _fs(spark, path)
    out = fs.create(p, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()


def exists(spark, path):
    fs, p = _fs(spark, path)
    return fs.exists(p)


def java_chain(throwable):
    """[{class, message}] for a Java throwable and its causes."""
    chain = []
    seen = 0
    while throwable is not None and seen < 10:
        chain.append({"class": throwable.getClass().getName(),
                      "message": str(throwable.getMessage())})
        throwable = throwable.getCause()
        seen += 1
    return chain


def python_chain(exc):
    """Exception chain for an error raised in the driver's Python code (load/start)."""
    chain = [{"class": type(exc).__name__, "message": str(exc)}]
    java = getattr(exc, "java_exception", None) or getattr(exc, "_origin", None)
    if java is not None:
        try:
            chain += java_chain(java)
        except Exception:  # noqa: BLE001 - best effort, the Python message is kept
            pass
    return chain


def query_chain(query):
    """Exception chain of a terminated query, or [] if it has none."""
    jexc = query._jsq.exception()
    return java_chain(jexc.get()) if jexc.isDefined() else []


def load(spark, options, schema):
    reader = spark.readStream.format("s3-connector").options(**options)
    if schema:
        reader = reader.schema(schema)
    return reader.load()


def run_stream(spark, cfg, loader=load):
    out = cfg["output"].rstrip("/")
    batches = []

    def handle(df, batch_id):
        # input_file_name() must be taken at the scan, before the cache, or it is empty.
        if not cfg.get("aggregate_by"):
            df = df.withColumn("_file", F.input_file_name())
        data = df.persist()
        try:
            files = ([] if cfg.get("aggregate_by")
                     else sorted(r[0] for r in data.select("_file").distinct().collect()))
            rows = data.count()
            # Overwrite per batch id: a replayed batch replaces its own output (idempotent sink).
            data.write.mode("overwrite").json("%s/batches/batch=%d" % (out, batch_id))
            # Written last, so a manifest means the batch output is complete.
            write_text(spark, "%s/manifests/%d.json" % (out, batch_id),
                       json.dumps({"batch_id": batch_id, "rows": rows, "files": files}))
            batches.append(batch_id)
        finally:
            data.unpersist()
        if batch_id == cfg.get("crash_after_batch"):
            # Simulated driver crash: sink written, batch not committed. Spark must replay it.
            os._exit(1)

    status = {"state": "starting", "exception": [], "batches": batches}
    query = None
    try:
        df = loader(spark, cfg["source_options"], cfg.get("schema"))
        if cfg.get("aggregate_by"):
            df = df.groupBy(cfg["aggregate_by"]).count()
            mode = "update"
        else:
            mode = "append"
        query = (df.writeStream
                 .outputMode(mode)
                 .option("checkpointLocation", cfg["checkpoint"])
                 .trigger(processingTime="%d seconds" % cfg.get("trigger_seconds", 5))
                 .foreachBatch(handle)
                 .start())
        write_text(spark, out + "/started.json", json.dumps({"id": str(query.id)}))
        deadline = time.time() + cfg.get("max_seconds", 600)
        while True:
            if query.awaitTermination(5):
                status["state"] = "failed" if query_chain(query) else "terminated"
                break
            if exists(spark, out + "/STOP"):
                query.stop()
                status["state"] = "stopped"
                break
            if time.time() > deadline:
                query.stop()
                status["state"] = "timeout"
                break
        status["exception"] = query_chain(query)
        status["last_progress"] = query.lastProgress
    except Exception as exc:  # noqa: BLE001 - recorded for the harness to assert on
        status["state"] = "failed"
        status["exception"] = python_chain(exc)
        status["traceback"] = traceback.format_exc()
        if query is not None:
            try:
                query.stop()
            except Exception:  # noqa: BLE001
                pass
    write_text(spark, out + "/status.json", json.dumps(status, default=str))


def run_cases(spark, cfg, loader=load):
    out = cfg["output"].rstrip("/")
    results = {}
    for case in cfg["cases"]:
        rows = []
        result = {"exception": [], "rows": 0}
        query = None
        try:
            df = loader(spark, case["options"], case.get("schema"))
            query = (df.writeStream
                     .option("checkpointLocation", "%s/checkpoints/%s" % (out, case["name"]))
                     .trigger(processingTime="2 seconds")
                     .foreachBatch(lambda d, _id: rows.append(d.count()))
                     .start())
            query.awaitTermination(case.get("seconds", 30))
            result["exception"] = query_chain(query)
            result["active_at_end"] = query.isActive
        except Exception as exc:  # noqa: BLE001 - this is what the case checks
            result["exception"] = python_chain(exc)
        finally:
            if query is not None:
                try:
                    query.stop()
                except Exception as exc:  # noqa: BLE001
                    result.setdefault("stop_error", str(exc))
        result["rows"] = sum(rows)
        results[case["name"]] = result
    write_text(spark, out + "/status.json",
               json.dumps({"state": "done", "cases": results}, default=str))


def main(argv, loader=load):
    """loader(spark, options, schema) -> streaming DataFrame; pyspark_job.py passes its own."""
    if len(argv) != 2:
        raise SystemExit("usage: %s <config.json s3 path>" % os.path.basename(argv[0]))
    spark = SparkSession.builder.appName("s3-connector-e2e").getOrCreate()
    cfg = json.loads(read_text(spark, argv[1]))
    if cfg["mode"] == "stream":
        run_stream(spark, cfg, loader)
    elif cfg["mode"] == "cases":
        run_cases(spark, cfg, loader)
    else:
        raise SystemExit("unknown mode %r" % cfg["mode"])
    spark.stop()


if __name__ == "__main__":
    main(sys.argv)
