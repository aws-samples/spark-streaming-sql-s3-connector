"""Read new S3 files announced by S3 event notifications in SQS, from PySpark.

spark-submit --jars spark-streaming-sql-s3-connector-0.0.3-spark<spark version>.jar read_stream.py \
    <sqs queue url> <aws region> <s3 root path> <checkpoint location> <output path>

Credentials come from the default AWS chain of the driver and executors
(instance profile, job role, environment). Do not put keys in options.
"""

import sys

from pyspark.sql import SparkSession

from spark_s3_connector import read_stream


def main(queue_url, region, root_path, checkpoint, output):
    spark = SparkSession.builder.appName("s3-connector-pyspark").getOrCreate()
    df = read_stream(
        spark,
        "valString STRING, valBoolean BOOLEAN, valDouble DOUBLE, valInt INT, valPartition STRING",
        file_format="csv",
        queue_url=queue_url,
        queue_region=region,
        max_files_per_trigger=500,
        max_file_age="15d",
        path_glob_filter=root_path.rstrip("/") + "/*.csv",  # matches the full s3:// path
        partition_columns=["valPartition"],
        base_path=root_path,
        sqs_long_polling_wait_time_seconds=5,
        sqs_visibility_timeout_seconds=60,
    )
    (
        df.writeStream.format("parquet")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="60 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main(*sys.argv[1:6])
