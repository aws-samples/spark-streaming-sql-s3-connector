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

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class S3ConnectorFileCacheSuite extends S3ConnectorTestBase {

  test("isNewFile result") {
    val cache = new S3ConnectorFileCache[String](5000)
    cache.add("/test/path1", QueueMessageDesc[String](1000, isProcessed = true, Some("handle1")))
    cache.add("/test/path2", QueueMessageDesc[String](1000, isProcessed = false, Some("handle2")))

    cache.isNewFile("/test/path0") shouldBe FileCacheNewFileResults.Ok
    cache.isNewFile("/test/path1") shouldBe FileCacheNewFileResults.ExistInCacheProcessed
    cache.isNewFile("/test/path2") shouldBe FileCacheNewFileResults.ExistInCacheNotProcessed

  }

  test("getUnprocessedFiles without sort") {
    val cache = new S3ConnectorFileCache[String](5000)

    cache.add("/test/path1", QueueMessageDesc[String](1000, isProcessed = false, Some("handle1")))
    cache.add("/test/path2", QueueMessageDesc[String](0, isProcessed = false, Some("handle2")))
    cache.add("/test/path3", QueueMessageDesc[String](200, isProcessed = false, Some("handle3")))


    val unprocessedFiles1 = cache.getUnprocessedFiles(None)

    unprocessedFiles1.toSet shouldEqual Set(
      FileMetadata("/test/path1", 1000, Some("handle1")),
      FileMetadata("/test/path2", 0, Some("handle2")),
      FileMetadata("/test/path3", 200, Some("handle3")))

    cache.markProcessed("/test/path2")
    val unprocessedFiles2 = cache.getUnprocessedFiles(None)

    unprocessedFiles2.toSet shouldEqual Set(
      FileMetadata("/test/path1", 1000, Some("handle1")),
      FileMetadata ("/test/path3", 200, Some("handle3")))

  }

  test("getUnprocessedFiles with sort") {
    val cache = new S3ConnectorFileCache[String](5000)

    cache.add("/test/path1", QueueMessageDesc[String](1000, isProcessed = false, Some("handle1")))
    cache.add("/test/path2", QueueMessageDesc[String](0, isProcessed = false, Some("handle2")))
    cache.add("/test/path3", QueueMessageDesc[String](200, isProcessed = false, Some("handle3")))


    val unprocessedFiles1 = cache.getUnprocessedFiles(None, shouldSortFiles = true)

    unprocessedFiles1 shouldEqual List(
      FileMetadata("/test/path2", 0, Some("handle2")),
      FileMetadata("/test/path3", 200, Some("handle3")),
      FileMetadata("/test/path1", 1000, Some("handle1")))
  }

  test("getUnprocessedFiles ignore old files") {
    val cache = new S3ConnectorFileCache[String](5000)

    cache.add("/test/path1", QueueMessageDesc[String](0, isProcessed = false, Some("handle1")))
    cache.add("/test/path2", QueueMessageDesc[String](1000, isProcessed = false, Some("handle2")))
    cache.add("/test/path3", QueueMessageDesc[String](6000, isProcessed = false, Some("handle3")))


    val unprocessedFiles = cache.getUnprocessedFiles(None)

    unprocessedFiles.toSet shouldEqual Set(
      FileMetadata("/test/path1", 0, Some("handle1")),
      FileMetadata("/test/path2", 1000, Some("handle2")),
      FileMetadata("/test/path3", 6000, Some("handle3")))

    cache.purge() // purge will move the latest timestamp

    cache.add("/test/path4", QueueMessageDesc[String](100, isProcessed = false, Some("handle4")))
    cache.add("/test/path5", QueueMessageDesc[String](200, isProcessed = false, Some("handle5")))

    val unprocessedFiles2 = cache.getUnprocessedFiles(None)

    unprocessedFiles2.toSet shouldEqual Set(
      FileMetadata("/test/path2", 1000, Some("handle2")),
      FileMetadata("/test/path3", 6000, Some("handle3")))
  }

  test("getUnprocessedFiles maxFilesPerTrigger without sort") {
    val cache = new S3ConnectorFileCache[String](5000)

    cache.add("/test/path1", QueueMessageDesc[String](1000, isProcessed = false, Some("handle1")))
    cache.add("/test/path2", QueueMessageDesc[String](0, isProcessed = false, Some("handle2")))
    cache.add("/test/path3", QueueMessageDesc[String](200, isProcessed = false, Some("handle3")))


    val unprocessedFiles1 = cache.getUnprocessedFiles(Some(2))

    unprocessedFiles1.toSet shouldEqual Set(
      FileMetadata("/test/path1", 1000, Some("handle1")),
      FileMetadata("/test/path2", 0, Some("handle2")))

    cache.markProcessed("/test/path2")
    val unprocessedFiles2 = cache.getUnprocessedFiles(Some(2))

    unprocessedFiles2.toSet shouldEqual Set(
      FileMetadata("/test/path1", 1000, Some("handle1")),
      FileMetadata("/test/path3", 200, Some("handle3")))
  }

  test("getUnprocessedFiles maxFilesPerTrigger with sort") {
    val cache = new S3ConnectorFileCache[String](5000)

    cache.add("/test/path1", QueueMessageDesc[String](1000, isProcessed = false, Some("handle1")))
    cache.add("/test/path2", QueueMessageDesc[String](0, isProcessed = false, Some("handle2")))
    cache.add("/test/path3", QueueMessageDesc[String](200, isProcessed = false, Some("handle3")))


    val unprocessedFiles1 = cache.getUnprocessedFiles(Some(2), shouldSortFiles = true)

    unprocessedFiles1 shouldEqual List(
      FileMetadata("/test/path2", 0, Some("handle2")),
      FileMetadata("/test/path3", 200, Some("handle3")))
  }

}

