package com.amazonaws.spark.sql.streaming.connector

import org.apache.spark.sql.execution.streaming

/**
 * Spark 3.x and 4.0 locations of the internal streaming classes the connector builds on.
 * Spark 4.1 moved them, see src/main/scala-spark-41 (selected by the spark-4.1 Maven profile).
 */
object SparkCompat {
  type MetadataLog[T] = streaming.MetadataLog[T]
  type CheckpointFileManager = streaming.CheckpointFileManager
  val CheckpointFileManager: streaming.CheckpointFileManager.type = streaming.CheckpointFileManager
  type FileEntry = streaming.FileStreamSource.FileEntry
  val FileEntry: streaming.FileStreamSource.FileEntry.type = streaming.FileStreamSource.FileEntry
  type FileStreamSourceOffset = streaming.FileStreamSourceOffset
  val FileStreamSourceOffset: streaming.FileStreamSourceOffset.type = streaming.FileStreamSourceOffset
}
