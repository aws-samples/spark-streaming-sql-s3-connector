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

package com.amazonaws.spark.sql.streaming.connector.metadataLog

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorFileCache
import com.amazonaws.spark.sql.streaming.connector.Utils.reportTimeTaken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.FileStreamSource.FileEntry
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.connector.s3.{RocksDB, RocksDBConf, RocksDBStateEncoder, S3SparkUtils}
import org.apache.spark.sql.streaming.connector.s3.S3SparkUtils.newDaemonSingleThreadScheduledExecutor
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class RocksDBS3SourceLog extends S3MetadataLog with Logging {

  import RocksDBS3SourceLog._

  private implicit val formats = Serialization.formats(NoTypeHints)

  var metadataLogVersion: Int = _
  var sparkSession: SparkSession = _
  var checkpointPath: String = _

  private val storeId : String = "RocksDBS3SourceLogStore"
  private val keySchema: StructType = StructType(
    Seq(StructField("type", ByteType, nullable = false), StructField("key", StringType, nullable = false))
  )
  private val valueSchema: StructType = StructType(
    Seq(StructField("value", BinaryType, nullable = false), StructField("timestamp", LongType, nullable = false))
  )

  private var sqlConf: SQLConf = _
  private var hadoopConf: Configuration = _
  private var fileCache: S3ConnectorFileCache[_] = _

  val maintenanceInterval: Long = MAINTENANCE_INTERVAL_DEFAULT_VALUE
  private var maintenanceTask: Option[MaintenanceTask] = None

  lazy val metadataPath = new Path(checkpointPath)

  private lazy val rocksDB = {
    val storeIdStr = s"SourceLogId(ver=${metadataLogVersion},storeId=${storeId})"
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val localRootDir = S3SparkUtils.createTempDir(S3SparkUtils.getLocalDir(sparkConf), storeIdStr)
    logInfo(s"localRootDir: ${localRootDir}")
    new RocksDB(checkpointPath, RocksDBConf(sqlConf), localRootDir, hadoopConf, storeIdStr)
  }

  protected lazy val fileManager: CheckpointFileManager =
    CheckpointFileManager.create(metadataPath, sparkSession.sessionState.newHadoopConf)

  override def init(sparkSession: SparkSession,
              checkpointPath: String,
              fileCache: S3ConnectorFileCache[_]): Unit = {

    this.metadataLogVersion = CURRENT_META_LOG_VERSION
    this.sparkSession = sparkSession
    this.checkpointPath = checkpointPath
    this.fileCache = fileCache
    this.hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    this.sqlConf = SQLConf.get

    if (!fileManager.exists(metadataPath)) {
      fileManager.mkdirs(metadataPath)
    }

    // Else uses -1 to make sure 0 is used as version when no previous batch
    reportTimeTaken("RocksDB load") {
      rocksDB.load(getLatestBatchId.getOrElse(-1L) + 1L)
    }

    startMaintenanceTask()
  }

  /** Start the periodic maintenance task if not already started */
  private def startMaintenanceTask(): Unit = {
    maintenanceTask = Some(new MaintenanceTask(
      maintenanceInterval,
      task = { doMaintenance() },
      onError = {}
    ))

    logInfo("RocksDBS3SourceLog maintenance task started")
  }

  private def doMaintenance(): Unit = {
    rocksDB.cleanup()
  }

  /**
   * Runs the `task` periodically and automatically cancels it if there is an exception. `onError`
   * will be called when an exception happens.
   */
  class MaintenanceTask(periodMs: Long, task: => Unit, onError: => Unit) {
    private val executor =
      newDaemonSingleThreadScheduledExecutor("RocksDBS3SourceLog-maintenance-task")

    private val runnable = new Runnable {
      override def run(): Unit = {
        try {
          task
        } catch {
          case NonFatal(e) =>
            logWarning("Error running RocksDBS3SourceLog maintenance thread", e)
            onError
        }
      }
    }

    private val future: ScheduledFuture[_] = executor.scheduleAtFixedRate(
      runnable, periodMs, periodMs, TimeUnit.MILLISECONDS)

    def stop(): Unit = {
      future.cancel(false)
      executor.shutdown()
    }

    def isRunning: Boolean = !future.isDone
  }

  def getLatestBatchId: Option[Long] = {
    reportTimeTaken("getLatestBatchId") {
      val maxId = fileManager.list(metadataPath, batchFilesFilter)
        .map(f => pathToBatchId(f.getPath))
        .sorted(Ordering.Long.reverse)
        .headOption

      // file id in checkpoint sources location is +1 than batch id
      maxId.map { id => id - 1 }
    }
  }

  override def getLatest(): Option[(Long, Array[FileEntry])] = {

    if (log.isTraceEnabled) {
      printAllBatchesInRocksDB()
    }

    getLatestBatchId.map { batchId =>
      val content = get(batchId).getOrElse {
        // If we find the last batch file, we must read that file, other than failing back to
        // old batches.
        throw new IllegalStateException(s"failed to read log file for batch $batchId")
      }
      (batchId, content)
    }
  }

  override def add(batchId: Long, fileEntries: Array[FileEntry]): Boolean = {
    add(batchId, fileEntries, None)
  }

  /**
   * Store the metadata for the specified batchId and return `true` if successful. If the batchId's
   * metadata has already been stored, this method will return `false`.
   */
  override def add(batchId: Long, fileEntries: Array[FileEntry], timestamp: Option[Long]): Boolean = {
    reportTimeTaken(s"RocksDBS3SourceLog.add batchId ${batchId}") {
      require(batchId >= 0, "batchId must not less than 0")
      require(fileEntries != null, "fileEntries cannot be 'null'")

      fileEntries.foreach { entry =>
        require(entry.batchId == batchId, s"${entry} batchId doesn't equal to ${batchId}")
      }

      val keyProj: UnsafeProjection = UnsafeProjection.create(keySchema)
      val prefixKeyProj: UnsafeProjection = UnsafeProjection.create(StructType(Seq(keySchema.head)))
      val valueProj: UnsafeProjection = UnsafeProjection.create(valueSchema)
      val batchKey = dataToKeyRow(KEY_BATCH_ID_TYPE, batchId.toString, keyProj)
      val batchValue = dataToBatchValueRow(fileEntries, timestamp, valueProj)
      val encoder: RocksDBStateEncoder = RocksDBStateEncoder.getEncoder(keySchema, valueSchema, 1)

      try {
        reportTimeTaken("RocksDBS3SourceLog cleanlog") {
          // Clean log before add a batch so that the latest batch is never removed
          cleanLog(fileCache.lastPurgeTimestamp, encoder, prefixKeyProj, keyProj)
        }

        val oldValue = rocksDB.put(encoder.encodeKey(batchKey),
          encoder.encodeValue(batchValue)
        )

        if(oldValue != null) {
          logError(s"Log batchId ${batchId} already exists.")
          rocksDB.rollback()
          false
        }
        else {
          fileEntries.foreach { entry =>
            val fileKey = dataToKeyRow(KEY_FILE_NAME_TYPE, entry.path, keyProj)
            val fileValue = dataToFileValueRow(entry, valueProj)
            rocksDB.put(encoder.encodeKey(fileKey),
              encoder.encodeValue(fileValue)
            )
          }

          reportTimeTaken("RocksDBS3SourceLog rocksDB.commit") {
            rocksDB.commit()
          }

          logInfo(s"RocksDBS3SourceLog.add for batchId ${batchId} added ${fileEntries.length} fileEntries.")
          logTrace(s"RocksDBS3SourceLog.add fileEntries for batchId ${batchId}: ${fileEntries.mkString}")

          if (log.isTraceEnabled) {
            printAllBatchesInRocksDB()
          }

          true
        }
      } catch {
        case NonFatal(e) =>
          Try(rocksDB.rollback()) match {
            case Failure(f) => e.addSuppressed(f)
            case Success(_) =>
          }
          logError(s"Error when add batch ${batchId}, " +
            s"fileEntries ${fileEntries.mkString("Array(", ", ", ")")}: ", e)
          throw e
      }
    }
  }

  def serialize[T <: AnyRef : ClassTag](value: T): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    Serialization.write[T](value, stream)
    stream.toByteArray
  }

  def deserialize[T <: AnyRef : ClassTag](bytes: Array[Byte]): T = {
    /** Needed to serialize type T into JSON when using Jackson */
    implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

    val bis = new ByteArrayInputStream(bytes)
    val reader = new InputStreamReader(bis, StandardCharsets.UTF_8)
    Serialization.read[T](reader)
  }

  def dataToPrefixKeyRow(t: Byte, prefixKeyProj: UnsafeProjection): UnsafeRow = {
    prefixKeyProj.apply(new GenericInternalRow(Array[Any](t))).copy()
  }

  def dataToKeyRow(t: Byte, k: String, keyProj: UnsafeProjection): UnsafeRow = {
    keyProj.apply(new GenericInternalRow(Array[Any](t, UTF8String.fromString(k)))).copy()
  }

  def dataToFileValueRow(e: FileEntry, valueProj: UnsafeProjection): UnsafeRow = {
    valueProj.apply(new GenericInternalRow(Array[Any](Array[Byte](), e.timestamp))).copy()
  }

  def dataToBatchValueRow(entries: Array[FileEntry], timestamp: Option[Long],
                          valueProj: UnsafeProjection): UnsafeRow = {
    val ts = timestamp match {
      case Some(v) => v
      case None => Instant.now.toEpochMilli
    }

    valueProj.apply(
      new GenericInternalRow(Array[Any](serialize[Array[FileEntry]](entries), ts))).copy()
  }

  override def isNewFile(path: String, lastPurgeTimestamp: Long): Boolean = {
    val fileInDb = getFile(path)
    val lastPurgeTs = lastPurgeTimestamp

    fileInDb match {
      case Some(fileTs) =>
        if (fileTs < lastPurgeTs) {
          // the file in RocksDB is too old and not tracking anymore
          logInfo(s"RocksDB files older than lastPurgeTimestamp: ${path}:${fileTs}:${lastPurgeTs}")
          true
        }
        else {
          logDebug(s"RocksDB existingFile: ${path}:${fileTs}:${lastPurgeTs}")
          false
        }
      case None =>
        logDebug(s"RocksDB isNewFile: - newFile: ${path}")
        true
    }
  }

  override def getFile(path: String): Option[Long] = {
    try {
      val encoder: RocksDBStateEncoder = RocksDBStateEncoder.getEncoder(keySchema, valueSchema, 1)
      val keyProj: UnsafeProjection = UnsafeProjection.create(keySchema)
      val fileKey = dataToKeyRow(KEY_FILE_NAME_TYPE, path, keyProj)
      val value = Option(rocksDB.get(encoder.encodeKey(fileKey)))

      value match {
        case Some(encodedRow) =>
          Some(encoder.decodeValue(encodedRow).getLong(1))
        case None =>
          logDebug(s"getFile ${path} not found in RocksDB.")
          None
      }
    } catch {
        case NonFatal(e) =>
          logError(s"getFile ${path} failed", e)
          throw e
    }
  }

  override def get(batchId: Long): Option[Array[FileEntry]] = {
    reportTimeTaken(s"RocksDBS3SourceLog.get batchId ${batchId}") {
      try {
        val encoder: RocksDBStateEncoder = RocksDBStateEncoder.getEncoder(keySchema, valueSchema, 1)
        val keyProj: UnsafeProjection = UnsafeProjection.create(keySchema)
        val batchKey = dataToKeyRow(KEY_BATCH_ID_TYPE, batchId.toString, keyProj)
        val batchValue = Option(rocksDB.get(encoder.encodeKey(batchKey)))

        batchValue match {
          case Some(encodedRow) =>
            val serializedRow = encoder.decodeValue(encodedRow)
            val fileEntries = deserialize[Array[FileEntry]](serializedRow.getBinary(0))
            logInfo(s"RocksDBS3SourceLog.get for batchId ${batchId} with ${fileEntries.length} fileEntries.")
            logTrace(s"RocksDBS3SourceLog.get fileEntries for batchId ${batchId}: ${fileEntries.mkString}")
            Some(fileEntries)
          case None =>
            logWarning(s"RocksDBS3SourceLog.get batchId not found in RocksDB: ${batchId} .")
            None
        }
      } catch {
        case NonFatal(e) =>
          logError(s"RocksDBS3SourceLog.get batchId failed: ${batchId}", e)
          throw e
      }
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, Array[FileEntry])] = {
    val startBatchId = startId.getOrElse(0L)
    val endBatchId = endId.orElse(getLatest().map(_._1)).getOrElse(0L)

    val batches = (startBatchId to endBatchId).map { id =>
      (id, get(id).getOrElse{throw new IllegalArgumentException(s"Log batch ${id} not found")})
    }.toArray

    batches
  }

  def logRocksDBMetrics(): Unit = {
    Try(rocksDB.metrics.json) match {
      case Success(metricsJson) =>
        logInfo(s"rocksDB metrics: ${metricsJson}")
      case Failure(e) =>
        logError("failed to get rocksDB.metrics.json", e)
    }
  }

  override def commit(): Unit = {
    // No op in commit as source logs are committed at every add to RocksDb,
    // log RocksDB Metrics instead.
    logRocksDBMetrics()
  }

  def printAllBatchesInRocksDB(): Unit = {
    try {
      val encoder: RocksDBStateEncoder = RocksDBStateEncoder.getEncoder(keySchema, valueSchema, 1)
      val prefixKeyProj: UnsafeProjection = UnsafeProjection.create(StructType(Seq(keySchema.head)))
      val prefix = dataToPrefixKeyRow(KEY_BATCH_ID_TYPE, prefixKeyProj)
      val prefixIter = rocksDB.prefixScan(encoder.encodePrefixKey(prefix))

      for (kv <- prefixIter) {

        val rowBatchId = encoder.decodeKey(kv.key)
        val batchId = rowBatchId.getUTF8String(1)
        val rowValue = encoder.decodeValue(kv.value)
        val batchTimestamp = rowValue.getLong(1)
        val fileEntries = deserialize[Array[FileEntry]](rowValue.getBinary(0))

        logInfo(s"printAllBatchesInRocksDB batch Id ${batchId}, batchTimestamp ${batchTimestamp}," +
          s" fileEntries size ${fileEntries.length}, fileEntries ${fileEntries.mkString}")

      }
    } catch {
      case NonFatal(e) =>
        logError("error when printAllBatchesInRocksDB in RocksDBS3SourceLog", e)
    }

  }

  private def cleanLog(lastPurgeTimestamp: Long,
               encoder: RocksDBStateEncoder,
               prefixKeyProj: UnsafeProjection,
               keyProj: UnsafeProjection
              ) : Unit = {

    val prefix = dataToPrefixKeyRow(KEY_BATCH_ID_TYPE, prefixKeyProj)
    val prefixIter = rocksDB.prefixScan(encoder.encodePrefixKey(prefix))

    var counter: Int = 0
    for (kv <- prefixIter if counter < MAX_BATCH_TO_REMOVE_PER_ADD) {
      try {
        val rowValue = encoder.decodeValue(kv.value)
        val batchTimestamp = rowValue.getLong(1)

        // Only remove the batch if batchTimestamp is earlier than lastPurgeTimestamp.
        // Since batchTimestamp is later than file timestamp, if it is possible that some files are not removed although
        // the file timestamp is earlier than lastPurgeTimestamp.
        if(batchTimestamp < lastPurgeTimestamp) {
          val rowBatchId = encoder.decodeKey(kv.key)
          val batchId = rowBatchId.getUTF8String(1)
          val fileEntries = deserialize[Array[FileEntry]](rowValue.getBinary(0))

          var latestFileTimestamp: Long = 0L
          fileEntries.foreach { entry =>
            latestFileTimestamp = latestFileTimestamp.max(entry.timestamp)
          }

          if (latestFileTimestamp < lastPurgeTimestamp) {
            rocksDB.remove(encoder.encodeKey(rowBatchId))

            fileEntries.foreach { entry =>
              val fileKey = dataToKeyRow(KEY_FILE_NAME_TYPE, entry.path, keyProj)

              rocksDB.remove(encoder.encodeKey(fileKey))
            }
            counter += 1
            logInfo(s"cleanLogTask removes logs for batchId ${batchId}")
          }
        }
      } catch {
        case NonFatal(e) => logInfo(s"Error when remove log from RocksDB", e)
      }
    }
  }

  override def close(): Unit = {
      if (maintenanceTask.isDefined) {
        maintenanceTask.get.stop()
        maintenanceTask = None
      }

    Try(rocksDB.close()) match {
      case Failure(e) =>
        logError("Failed to close rocksDB.", e)
        throw e
      case Success(_) => logDebug("rocksDB closed successfully.")
    }
  }


  // removal of old entries are done by logCleaner
  override def purge(thresholdBatchId: Long): Unit = {
    throw new UnsupportedOperationException("RocksDB3SourceLog purge not supported")
  }
}

object RocksDBS3SourceLog {
  val CURRENT_META_LOG_VERSION: Int = 0

  val KEY_FILE_NAME_TYPE: Byte = 1
  val KEY_BATCH_ID_TYPE: Byte = 2

  val MAX_BATCH_TO_REMOVE_PER_ADD: Int = 20
  val MAINTENANCE_INTERVAL_DEFAULT_VALUE: Long = 5000

  val FILE_SUFFIX = ".zip"

  /**
   * A `PathFilter` to filter only batch files
   */
  val batchFilesFilter: PathFilter = (path: Path) => isBatchFile(path)

  def pathToBatchId(path: Path): Long = {
    path.getName.split("\\.")(0).toLong
  }

  def isBatchFile(path: Path): Boolean = {
    try {

      if (path.getName.endsWith(FILE_SUFFIX)) {
        pathToBatchId(path)
        true
      }
      else {
        false
      }
    } catch {
      case _: NumberFormatException => false
    }
  }
}
