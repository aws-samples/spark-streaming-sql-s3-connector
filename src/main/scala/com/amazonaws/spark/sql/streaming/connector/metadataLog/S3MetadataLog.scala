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

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorFileCache

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.FileEntry
import org.apache.spark.sql.execution.streaming.MetadataLog

trait S3MetadataLog extends MetadataLog[Array[FileEntry]]{
  /**
   * Returns true if we should consider this file a new file. The file is only considered "new"
   * if it is new enough that we are still tracking, and we have not seen it before.
   */
  def isNewFile(path: String, lastPurgeTimestamp: Long): Boolean

  def getFile(path: String): Option[Long]

  def init(sparkSession: SparkSession,
           checkpointPath: String,
           fileCache: S3ConnectorFileCache[_]
          ): Unit

  def add(batchId: Long, fileEntries: Array[FileEntry], timestamp: Option[Long]): Boolean

  def commit(): Unit

  def close(): Unit
}
