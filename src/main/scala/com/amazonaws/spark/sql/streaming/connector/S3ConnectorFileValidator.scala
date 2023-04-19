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

import com.amazonaws.spark.sql.streaming.connector.FileValidResults.FileValidResult
import com.amazonaws.spark.sql.streaming.connector.metadataLog.S3MetadataLog
import org.apache.hadoop.fs.GlobPattern

import org.apache.spark.internal.Logging

class S3ConnectorFileValidator (sourceOptions: S3ConnectorSourceOptions,
                                fileCache: S3ConnectorFileCache[_],
                                metadataLog: S3MetadataLog) extends Logging {

  private val globber = sourceOptions.pathGlobFilter.map(new GlobPattern(_))

  def isValidNewFile(filePath: String, timestamp: Long) : FileValidResult = {
    val lastPurgeTimestamp = fileCache.lastPurgeTimestamp
    globber.map( p => p.matches(filePath) ) match {
      case Some(true) | None =>
        if (timestamp < lastPurgeTimestamp) {
          logInfo(s"isValidNewFile ${filePath} has ts ${timestamp} " +
            s"is older than ${lastPurgeTimestamp}")
          FileValidResults.FileExpired
        } else {
          val cacheResult = fileCache.isNewFile(filePath)

          if (cacheResult == FileCacheNewFileResults.ExistInCacheProcessed) {
            FileValidResults.ExistInCacheProcessed
          } else if (cacheResult == FileCacheNewFileResults.ExistInCacheNotProcessed) {
            FileValidResults.ExistInCacheNotProcessed
          } else if (!metadataLog.isNewFile(filePath, lastPurgeTimestamp)) {
            FileValidResults.PersistedInMetadataLog
          } else {
            FileValidResults.Ok
          }
        }
      case Some(false) => FileValidResults.PatternNotMatch
    }
  }
}

object FileValidResults extends Enumeration {
  type FileValidResult = Value
  val Ok, PatternNotMatch, FileExpired, ExistInCacheProcessed, ExistInCacheNotProcessed, PersistedInMetadataLog = Value
}
