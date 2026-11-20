package com.amazonaws.spark.sql.streaming.connector

import org.apache.spark.sql.execution.streaming.{checkpointing, runtime}

/** Spark 4.1 locations of the internal streaming classes, see src/main/scala-spark-pre41 for older Spark. */
object SparkCompat {
  type MetadataLog[T] = checkpointing.MetadataLog[T]
  type CheckpointFileManager = checkpointing.CheckpointFileManager
  val CheckpointFileManager: checkpointing.CheckpointFileManager.type = checkpointing.CheckpointFileManager
  type FileEntry = runtime.FileStreamSource.FileEntry
  val FileEntry: runtime.FileStreamSource.FileEntry.type = runtime.FileStreamSource.FileEntry
  type FileStreamSourceOffset = runtime.FileStreamSourceOffset
  val FileStreamSourceOffset: runtime.FileStreamSourceOffset.type = runtime.FileStreamSourceOffset
}
