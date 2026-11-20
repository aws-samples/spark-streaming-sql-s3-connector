"""Spark driver for the PySpark e2e tests: same modes as stream_job.py, but every source is
built with the PySpark helper package (python/src/spark_s3_connector) instead of raw option keys.

Usage (EMR Serverless): entryPoint pyspark_job.py, --py-files pyspark_deps.zip (holds
spark_s3_connector/ and stream_job.py), --jars <connector jar>, argument: the config.json path.

The config's "source_options" (stream mode) and each case's "options" (cases mode) are the
keyword arguments of spark_s3_connector.read_stream(), e.g. {"file_format": "csv",
"queue_url": ..., "queue_region": ..., "max_files_per_trigger": 1}.
"""
from __future__ import annotations

import sys

import stream_job
from spark_s3_connector import read_stream


def load(spark, kwargs, schema):
    return read_stream(spark, schema, **kwargs)


if __name__ == "__main__":
    stream_job.main(sys.argv, loader=load)
