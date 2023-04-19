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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

import com.amazonaws.spark.sql.streaming.connector.FileCacheNewFileResults.FileCacheNewFileResult
import com.amazonaws.spark.sql.streaming.connector.Utils.reportTimeTaken

import org.apache.spark.internal.Logging

/**
 * A custom hash map used to track the list of files not processed yet. This map is thread-safe.
 */

class S3ConnectorFileCache[T](maxFileAgeMs: Long) extends Logging {

  require(maxFileAgeMs >= 0)

  /** Mapping from file path to its message description. */
  private val fileMap = new ConcurrentHashMap[String, QueueMessageDesc[T]]

  /** Timestamp for the last purge operation. */
  def lastPurgeTimestamp: Long = _lastPurgeTimestamp.get()
  private val _lastPurgeTimestamp: AtomicLong = new AtomicLong(0L)

  /** Timestamp of the latest file. */
  private def latestTimestamp: Long = _latestTimestamp.get()
  private val _latestTimestamp: AtomicLong = new AtomicLong(0L)

  private def setAtomicTimestamp(atomicTs: AtomicLong, newTs: Long): Unit = {
    breakable {
      while (true) {
        val oldTs = atomicTs.get()
        if (newTs > oldTs) {
          val success = atomicTs.compareAndSet(oldTs, newTs)
          if (success) break
        }
        else {
          break
        }
      }
    }
  }

  /** Add a new file to the map. */
  def add(path: String, fileStatus: QueueMessageDesc[T]): Unit = {
    fileMap.put(path, fileStatus)
    setAtomicTimestamp(_latestTimestamp, fileStatus.timestampMs)
  }

  def addIfAbsent(path: String, fileStatus: QueueMessageDesc[T]): QueueMessageDesc[T] = {
    val ret = fileMap.computeIfAbsent(path, _ => fileStatus)
    setAtomicTimestamp(_latestTimestamp, fileStatus.timestampMs)

    ret
  }

  def isNewFile(path: String): FileCacheNewFileResult = {
    val fileMsg = fileMap.get(path)
    val isNew = if (fileMsg == null) FileCacheNewFileResults.Ok
                else if (fileMsg.isProcessed) FileCacheNewFileResults.ExistInCacheProcessed
                else FileCacheNewFileResults.ExistInCacheNotProcessed

    logDebug(s"fileCache isNewFile for ${path}: ${isNew}")
    isNew
  }

  /**
   * Returns all the new files found - ignore processed and aged files.
   */
  def getUnprocessedFiles(maxFilesPerTrigger: Option[Int],
                          shouldSortFiles: Boolean = false): Seq[FileMetadata[T]] = {
    reportTimeTaken("File cache getUnprocessedFiles") {
      if (shouldSortFiles) {
        val uncommittedFiles = filterAllUnprocessedFiles()
        val sortedFiles = reportTimeTaken("Sorting Files") {
          uncommittedFiles.sortWith(_.timestampMs < _.timestampMs)
        }

        maxFilesPerTrigger match {
          case Some(maxFiles) =>
            sortedFiles
              .filter(file => file.timestampMs >= lastPurgeTimestamp)
              .take(maxFiles)
          case None => sortedFiles
        }
      } else {
        maxFilesPerTrigger match {
          case Some(maxFiles) => filterTopUnprocessedFiles(maxFiles)
          case None => filterAllUnprocessedFiles()
        }
      }
    }
  }

  private def filterTopUnprocessedFiles(maxFilesPerTrigger: Int): List[FileMetadata[T]] = {
    val iterator = fileMap.asScala.iterator
    val uncommittedFiles = ArrayBuffer[FileMetadata[T]]()
    while (uncommittedFiles.length < maxFilesPerTrigger && iterator.hasNext) {
      val file = iterator.next()

      if (!file._2.isProcessed && file._2.timestampMs >= lastPurgeTimestamp) {
        uncommittedFiles += FileMetadata(file._1, file._2.timestampMs, file._2.messageId)
      }
    }
    uncommittedFiles.toList
  }

  private def filterAllUnprocessedFiles(): List[FileMetadata[T]] = {
    fileMap.asScala.foldLeft(List[FileMetadata[T]]()) {
      (list, file) =>
        if (!file._2.isProcessed && file._2.timestampMs >= lastPurgeTimestamp) {
          list :+ FileMetadata[T](file._1, file._2.timestampMs, file._2.messageId)
        } else {
          list
        }
    }
  }

  /** Removes aged and processed entries and returns the number of files removed. */
  def purge(): Int = {
    setAtomicTimestamp(_lastPurgeTimestamp, latestTimestamp - maxFileAgeMs)
    var count = 0
    fileMap.asScala.foreach { fileEntry =>
      if (fileEntry._2.timestampMs < lastPurgeTimestamp
        || fileEntry._2.isProcessed
      ) {
        fileMap.remove(fileEntry._1)
        count += 1
      }
    }
    count
  }

  /** Mark file entry as committed or already processed */
  def markProcessed(path: String): Unit = {
    fileMap.replace(path, QueueMessageDesc(
      fileMap.get(path).timestampMs, isProcessed = true, fileMap.get(path).messageId))
  }

  def size: Int = fileMap.size()

}

object FileCacheNewFileResults extends Enumeration {
  type FileCacheNewFileResult = Value
  val Ok, ExistInCacheNotProcessed, ExistInCacheProcessed = Value
}

