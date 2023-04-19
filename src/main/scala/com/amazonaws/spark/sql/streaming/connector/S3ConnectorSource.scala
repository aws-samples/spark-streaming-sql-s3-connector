/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.spark.sql.streaming.connector

import java.net.URI

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSource.REPROCESS_LOCK_FILE
import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions.SQS_QUEUE
import com.amazonaws.spark.sql.streaming.connector.Utils.reportTimeTaken
import com.amazonaws.spark.sql.streaming.connector.client.{AsyncQueueClient, AsyncSqsClientBuilder}
import com.amazonaws.spark.sql.streaming.connector.metadataLog.{RocksDBS3SourceLog, S3MetadataLog}
import org.apache.hadoop.fs.{FSDataOutputStream, GlobPattern, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.FileStreamSource._
import org.apache.spark.sql.streaming.connector.s3.S3SparkUtils
import org.apache.spark.sql.types.StructType

class S3ConnectorSource[T](sparkSession: SparkSession,
                        metadataPath: String,
                        options: Map[String, String],
                        userSchema: Option[StructType])
  extends Source with Logging {

  private val sourceOptions = S3ConnectorSourceOptions(options)

  override def schema: StructType = userSchema.getOrElse(
    throw new S3ConnectorNoSchemaException
  )

  private val fileCache = new S3ConnectorFileCache[T](sourceOptions.maxFileAgeMs)

  private val metadataLog: S3MetadataLog = sourceOptions.queueType match {
    case SQS_QUEUE => new RocksDBS3SourceLog()
    case _ => throw new S3ConnectorUnsupportedQueueTypeException(
      s"Unsupported queue type: ${sourceOptions.queueType}")
  }

  metadataLog.init(sparkSession, metadataPath, fileCache)

  private var metadataLogCurrentOffset = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  private val partitionColumns: Seq[String] = sourceOptions.partitionColumns match {
    case Some(columns) => columns.split(",").map(_.trim)
    case None => Seq.empty
  }

  private val fileValidator = new S3ConnectorFileValidator(sourceOptions, fileCache, metadataLog)

  private val queueClient: AsyncQueueClient[T] = sourceOptions.queueType match {
    case SQS_QUEUE =>
      var sqsClient: AsyncQueueClient[T] = null

      sqsClient = new AsyncSqsClientBuilder()
      .sourceOptions(sourceOptions)
      .consumer(
        (msg: FileMetadata[T]) => {
          val validateResult = fileValidator.isValidNewFile(msg.filePath, msg.timestampMs)
          if ( validateResult == FileValidResults.Ok) {
            logDebug(s"SQS message consumer file add to cache: ${msg}")
            val msgDesc = QueueMessageDesc[T](msg.timestampMs, isProcessed = false, msg.messageId)
            val result = fileCache.addIfAbsent(msg.filePath, msgDesc)
            if (result != msgDesc) {
              // This could happen as isValidFile check and adding to fileCache are not atomic.
              // Don't delete the message, let it retry instead.
              logWarning(s"SQS message consumer the message was not added to cache: ${msg}," +
                s"as same path already exist in cache: ${result}. The message will be retried.")
              msg.messageId.map(sqsClient.setMessageVisibility(_, sourceOptions.sqsVisibilityTimeoutSeconds))
            }
          } else if (validateResult == FileValidResults.ExistInCacheNotProcessed) {
            // This can happen when the filePath is not processed/persisted yet but visibilityTimeout.
            // Do not delete the message. Let it retry until the filePath is persisted.
            logWarning(s"SQS message consumer file already exists in cache: ${msg}.")
            msg.messageId.map(sqsClient.setMessageVisibility(_, sourceOptions.sqsVisibilityTimeoutSeconds))
          } else {
            logWarning(s"SQS message consumer delete msg of invalid file: ${msg}." +
              s" Reason: ${validateResult}")
            msg.messageId.map(sqsClient.deleteInvalidMessageIfNecessary(_))
          }
        }
      )
      .build()
      sqsClient
    case _ => throw new S3ConnectorUnsupportedQueueTypeException(
      s"Unsupported queue type: ${sourceOptions.queueType}")
  }

  purgeCache()

  logInfo(s"maxFilesPerBatch = ${sourceOptions.maxFilesPerTrigger}, maxFileAgeMs = ${sourceOptions.maxFileAgeMs}")

  for (reprocessStarId <- sourceOptions.reprocessStartBatchId;
       reprocessEndId <- sourceOptions.reprocessEndBatchId
  ) yield handleReprocessing(reprocessStarId, reprocessEndId)

  private def handleReprocessing(startLogId: Int, endLogId: Int): Unit = {
    sourceOptions.reprocessState match {
      case ReprocessStates.DryRun =>
        logInfo(s"Reprocess dry run batch start ${startLogId}, end ${endLogId}." +
          s"Following files to be reprocessed")
        val files = getMetadataLogByRange(startLogId, endLogId)
        files.foreach( file => logInfo(file.productIterator.mkString("\t")))
        throw new S3ConnectorReprocessDryRunException(s"Get ${endLogId -startLogId +1} batches," +
          s" ${files.length} files. Reprocess dry run completed. S3ConnectorReprocessDryRunException to exit.")
      case ReprocessStates.InAction =>
        val fs = new Path(metadataPath).getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
        var os: FSDataOutputStream = null
        try {
          val lockFile = new Path(metadataPath + REPROCESS_LOCK_FILE)
          if (fs.exists(lockFile)) {
            throw new S3ConnectorReprocessLockExistsException(s"${lockFile} already exists." +
              s"Remove it and rerun the reprocessing.")
          }
          os = fs.create(lockFile)

          val files = getMetadataLogByRange(startLogId, endLogId)
          files.foreach { file =>
            fileCache.add(file.path, QueueMessageDesc(file.timestamp, isProcessed = false, None))
          }
        } catch {
          case le: S3ConnectorReprocessLockExistsException => throw le
          case NonFatal(e) =>
            val reprocessException = new S3ConnectorReprocessException("Error in reprocessing")
            reprocessException.addSuppressed(e)
            throw reprocessException
        }
        finally {
          if (os != null) os.close()
        }

      case _ => logWarning("Reprocess skipped")

    }
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`].
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = start.map(FileStreamSourceOffset(_).logOffset).getOrElse(-1L)
    val endOffset = FileStreamSourceOffset(end).logOffset

    assert(startOffset <= endOffset)

    val files = getMetadataLogByRange(startOffset + 1, endOffset) // startOffset is exclusive

    logInfo(s"getBatch processing ${files.length} files from ${startOffset + 1}:$endOffset")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))

    val newDataSource =
      DataSource(
        sparkSession,
        paths = files.map(f => new Path(new URI(f.path)).toString),
        userSpecifiedSchema = Some(schema),
        partitionColumns = partitionColumns,
        className = sourceOptions.fileFormat,
        options = options)

    S3SparkUtils.ofRows(sparkSession, LogicalRelation(newDataSource.resolveRelation(
      checkFilesExist = false), isStreaming = true))
  }

  /*
  * both startId and endId are inclusive
   */
  private def getMetadataLogByRange(startId: Timestamp, endId: Timestamp) = {
    val globber = sourceOptions.pathGlobFilter.map(new GlobPattern(_))
    metadataLog.get(Some(startId), Some(endId))
      .flatMap(_._2)
      .filter { file => globber.forall(p => p.matches(file.path)) }
  }

  private def fetchMaxOffset(): FileStreamSourceOffset = {

    // only fetch new messages from SQS when not reprocessing
    if (sourceOptions.reprocessState == ReprocessStates.NoReprocess) {
      // If asyncFetch can't finish in time, it continues in a separate thread.
      // And here proceeds with what's available in the fileCache
      queueClient.asyncFetch(sourceOptions.queueFetchWaitTimeoutSeconds)
    }


    val batchFiles = fileCache.getUnprocessedFiles(sourceOptions.maxFilesPerTrigger)

    if (batchFiles.nonEmpty) {
      metadataLogCurrentOffset += 1
      val addSuccess = metadataLog.add(metadataLogCurrentOffset, batchFiles.map {
        case FileMetadata(path, timestamp, _) =>
          FileEntry(path = path, timestamp = timestamp, batchId = metadataLogCurrentOffset)
      }.toArray)

      if (addSuccess) {
        logInfo(s"Log offset set to $metadataLogCurrentOffset with ${batchFiles.size} new files")
        val messageIds = batchFiles.map {
          case FileMetadata(path, _, messageId) =>
            fileCache.markProcessed(path)
            logDebug(s"New file in fetchMaxOffset: $path")
            messageId
        }.toList

        queueClient.handleProcessedMessageBatch(messageIds.flatten)
      }
      else {
        throw new S3ConnectorMetalogAddException(s"BatchId ${metadataLogCurrentOffset} already exists.")
      }

    }
    else if (sourceOptions.reprocessState != ReprocessStates.NoReprocess) {
      logWarning("This is a reprocessing run. No new data are fetched from the queue."
        + " To resume the new data processing, restart the application without reprocessing parameters.")
    }

    val numPurged = purgeCache()

    logDebug(
      s"""
         |Number of files selected for batch = ${batchFiles.size}
         |Number of files purged from tracking map = ${numPurged.getOrElse(0)}
       """.stripMargin)

    FileStreamSourceOffset(metadataLogCurrentOffset)
  }

  override def getOffset: Option[Offset] = reportTimeTaken ("getOffset") {
    Some(fetchMaxOffset()).filterNot(_.logOffset == -1)
  }

  override def commit(end: Offset): Unit = {
    purgeCache()
    metadataLog.commit()
    logQueueClientMetrics()
  }

  override def stop(): Unit = {
    try {
      queueClient.close()
    }
    finally {
      metadataLog.close()
    }

    logQueueClientMetrics()
  }

  override def toString: String = s"S3ConnectorSource[${queueClient.queueUrl}]"

  private def purgeCache(): Option[Int] = {
    Try(fileCache.purge()) match {
      case Success(cnt) =>
        logDebug(s"Successfully purged ${cnt} entries in sqsFileCache")
        Some(cnt)
      case Failure(e) =>
        logError("failed to purge fileCache", e)
        None
    }
  }

  private def logQueueClientMetrics(): Unit = {
    Try(queueClient.metrics.json) match {
      case Success(metricsString) =>
        logInfo(s"queueClient metrics: ${metricsString}")
      case Failure(e) =>
        logError("failed to get queueClient.metrics", e)
    }
  }

}

object S3ConnectorSource {
  val REPROCESS_LOCK_FILE = "/reprocess.lock"
}


