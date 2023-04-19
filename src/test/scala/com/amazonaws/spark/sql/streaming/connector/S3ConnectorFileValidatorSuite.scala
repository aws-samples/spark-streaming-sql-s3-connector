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

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions.PATH_GLOB_FILTER
import com.amazonaws.spark.sql.streaming.connector.TestUtils.doReturnMock
import com.amazonaws.spark.sql.streaming.connector.metadataLog.S3MetadataLog
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.mock
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class S3ConnectorFileValidatorSuite  extends S3ConnectorTestBase {


  val defaultTs: Long = 1000L

  val mockFileCache = mock(classOf[S3ConnectorFileCache[String]])
  val mockMetadataLog = mock(classOf[S3MetadataLog])

  doReturnMock(FileCacheNewFileResults.Ok).when(mockFileCache).isNewFile(any())
  doReturnMock(100L).when(mockFileCache).lastPurgeTimestamp
  doReturnMock(true).when(mockMetadataLog).isNewFile(any(), any())

  test("isValidNewFile fileFilter not set") {
    doReturnMock(FileCacheNewFileResults.ExistInCacheNotProcessed).when(mockFileCache).isNewFile(meq("/path/test1"))
    doReturnMock(FileCacheNewFileResults.ExistInCacheProcessed).when(mockFileCache).isNewFile(meq("/path/test2"))
    doReturnMock(false).when(mockMetadataLog).isNewFile(meq("/path/test3"), any())

    val validator = new S3ConnectorFileValidator(
      S3ConnectorSourceOptions(defaultOptionMap),
      mockFileCache, mockMetadataLog
    )

    validator.isValidNewFile("/path/test0", defaultTs) shouldBe FileValidResults.Ok
    validator.isValidNewFile("/path/test1", defaultTs) shouldBe FileValidResults.ExistInCacheNotProcessed
    validator.isValidNewFile("/path/test2", defaultTs) shouldBe FileValidResults.ExistInCacheProcessed
    validator.isValidNewFile("/path/test3", defaultTs) shouldBe FileValidResults.PersistedInMetadataLog
    validator.isValidNewFile("/path/test1", 10L) shouldBe FileValidResults.FileExpired
  }


  test("isValidNewFile fileFilter set to *.csv") {
    val validator = new S3ConnectorFileValidator(
      S3ConnectorSourceOptions(defaultOptionMap + (PATH_GLOB_FILTER -> "*1.csv")),
      mockFileCache, mockMetadataLog
    )

    validator.isValidNewFile("/path/test1", defaultTs) shouldBe FileValidResults.PatternNotMatch
    validator.isValidNewFile("/path/test1.csv", defaultTs) shouldBe FileValidResults.Ok
    validator.isValidNewFile("/path/test21.csv", defaultTs) shouldBe FileValidResults.Ok
    validator.isValidNewFile("/path/test2.csv", defaultTs) shouldBe FileValidResults.PatternNotMatch
    validator.isValidNewFile("/path/test1.json", defaultTs) shouldBe FileValidResults.PatternNotMatch

  }


  test("isValidNewFile fileFilter set to partition=1*") {
    val validator = new S3ConnectorFileValidator(
      S3ConnectorSourceOptions(defaultOptionMap + (PATH_GLOB_FILTER -> "*/partition=1*/*")),
      mockFileCache, mockMetadataLog
    )

    validator.isValidNewFile("/path/partition=1/test1", defaultTs) shouldBe FileValidResults.Ok
    validator.isValidNewFile("/path/partition=11/test1.csv", defaultTs) shouldBe FileValidResults.Ok
    validator.isValidNewFile("/path/partition=2/test1.csv", defaultTs) shouldBe FileValidResults.PatternNotMatch

  }
}
