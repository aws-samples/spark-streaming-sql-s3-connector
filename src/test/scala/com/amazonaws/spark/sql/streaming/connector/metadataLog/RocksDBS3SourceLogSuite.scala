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

import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import com.amazonaws.spark.sql.streaming.connector.{QueueMessageDesc, S3ConnectorFileCache, S3ConnectorTestBase}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.execution.streaming.FileStreamSource.FileEntry
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.SpanSugar.convertLongToGrainOfTime

import java.io.File


class RocksDBS3SourceLogSuite extends S3ConnectorTestBase with SharedSparkSession{

  var testS3SourceLog: RocksDBS3SourceLog = _
  var fileCache: S3ConnectorFileCache[String] = _

  before {
    testS3SourceLog = new RocksDBS3SourceLog()
  }

  after {
    logInfo("close testS3SourceLog")
    testS3SourceLog.close()
  }

  private def listBatchFiles(fs: FileSystem, sourceLog: RocksDBS3SourceLog): Set[String] = {
    fs.listStatus(sourceLog.metadataPath).map(_.getPath).filter { fileName =>
      try {
        com.amazonaws.spark.sql.streaming.connector.metadataLog.RocksDBS3SourceLog.isBatchFile(fileName)
      } catch {
        case _: NumberFormatException => false
      }
    }.map(_.getName).toSet
  }

  def initTestS3SourceLog(parentDir: File): Unit = {
    val checkpointDir = s"${parentDir}/checkpoint/"
    fileCache = new S3ConnectorFileCache(1000)
    testS3SourceLog.init(spark, checkpointDir, fileCache)
  }

  test("delete expired logs") {
    withSQLConf(
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "2") {
      withTestTempDir { tempDir =>
        initTestS3SourceLog(tempDir)
        val fs = testS3SourceLog.metadataPath.getFileSystem(spark.sessionState.newHadoopConf())
        val waitTime = 3 * testS3SourceLog.maintenanceInterval
        def listBatchFiles(): Set[String] = this.listBatchFiles(fs, testS3SourceLog)

        testS3SourceLog.add(0, Array(FileEntry("/test/path0", 1000, 0)))
        eventually(timeout(waitTime.milliseconds)) {
          assert(Set("1.zip") === listBatchFiles())
        }

        testS3SourceLog.add(1, Array(FileEntry("/test/path1", 2000, 1)))
        eventually(timeout(waitTime.milliseconds)) {
          assert(Set("1.zip", "2.zip") === listBatchFiles())
        }

        testS3SourceLog.add(2, Array(FileEntry("/test/path2", 3000, 2)))
        eventually(timeout(waitTime.milliseconds)) {
          assert(Set("2.zip", "3.zip") === listBatchFiles())
        }
      }
    }
  }

  test("add S3 Source logs successfully") {
    withTestTempDir { tempDir =>
      initTestS3SourceLog(tempDir)

      val fileArray = Array(
        FileEntry("/test/path1", 1000, 0),
        FileEntry("/test/path2", 2000, 0),
      )

      val addResult = testS3SourceLog.add(0, fileArray)
      addResult shouldBe true

      val getResult = testS3SourceLog.get(0)
      getResult.get shouldEqual fileArray
    }
  }

  test("add duplicate batch should return false") {
    withTestTempDir { tempDir =>
      initTestS3SourceLog(tempDir)

      val fileArray = Array(
        FileEntry("/test/path1", 1000, 0),
        FileEntry("/test/path2", 2000, 0),
      )

      val addResult = testS3SourceLog.add(0, fileArray)
      addResult shouldBe true

      val addResult2 = testS3SourceLog.add(0, fileArray)
      addResult2 shouldBe false
    }


  }
  test("get range success") {
    withTestTempDir { tempDir =>
      initTestS3SourceLog(tempDir)

      val fileArray1 = Array(
        FileEntry("/test/path1", 1000, 0),
        FileEntry("/test/path2", 1000, 0),
      )

      val fileArray2 = Array(
        FileEntry("/test2/path1", 2000, 1),
        FileEntry("/test2/path2", 2000, 1),
      )

      val fileArray3 = Array(
        FileEntry("/test3/path1", 3000, 2),
        FileEntry("/test3/path2", 3000, 2),
      )

      testS3SourceLog.add(0, fileArray1)
      testS3SourceLog.add(1, fileArray2)
      testS3SourceLog.add(2, fileArray3)

      val getResult = testS3SourceLog.get(Some(0), Some(2))
      getResult(0)._1 shouldEqual 0
      getResult(0)._2 shouldEqual fileArray1
      getResult(1)._1 shouldEqual 1
      getResult(1)._2 shouldEqual fileArray2
      getResult(2)._1 shouldEqual 2
      getResult(2)._2 shouldEqual fileArray3

      val getResult2 = testS3SourceLog.get(None, None)
      getResult2(0)._1 shouldEqual 0
      getResult2(0)._2 shouldEqual fileArray1
      getResult2(1)._1 shouldEqual 1
      getResult2(1)._2 shouldEqual fileArray2
      getResult2(2)._1 shouldEqual 2
      getResult2(2)._2 shouldEqual fileArray3
    }
  }

  test("get range exception") {
    withTestTempDir { tempDir =>
      initTestS3SourceLog(tempDir)

      val fileArray1 = Array(
        FileEntry("/test/path1", 1000, 0),
        FileEntry("/test/path2", 1000, 0),
      )

      testS3SourceLog.add(0, fileArray1)
      val exception = intercept[IllegalArgumentException] {
        testS3SourceLog.get(Some(0), Some(1))
      }

      exception.getMessage should include ("batch 1 not found")
    }
  }

  test("check isNewFile or not") {
    withTestTempDir { tempDir =>
      initTestS3SourceLog(tempDir)

      val fileArray = Array(
        FileEntry("/test/path1", 1000, 0),
        FileEntry("/test/path2", 2000, 0),
      )

      val newFile = FileEntry("/test/path3", 3000, 1)

      testS3SourceLog.add(fileArray(0).batchId, fileArray)

      val result1 = testS3SourceLog.isNewFile(fileArray(0).path, fileArray(0).timestamp - 5000)
      val result2 = testS3SourceLog.isNewFile(newFile.path, newFile.timestamp - 5000)

      result1 shouldBe false
      result2 shouldBe true

      testS3SourceLog.add(newFile.batchId, Array(newFile))

      val result3 = testS3SourceLog.isNewFile(newFile.path, newFile.timestamp - 5000)
      result3 shouldBe false

      val result4 = testS3SourceLog.isNewFile(fileArray(0).path, 3000)
      result4 shouldBe true

      val result5 = testS3SourceLog.isNewFile(newFile.path, 3000)
      result5 shouldBe false
    }
  }

  test("verify old logs are cleaned") {
    withTestTempDir { tempDir =>
      initTestS3SourceLog(tempDir)

      val fileArray1 = Array(
        FileEntry("/test/path11", 100, 1),
        FileEntry("/test/path12", 100, 1),
      )

      val fileArray2 = Array(
        FileEntry("/test/path21", 200, 2),
        FileEntry("/test/path22", 200, 2),
      )

      val fileArray6 = Array(
        FileEntry("/test/path61", 300, 6),
        FileEntry("/test/path62", 600, 6),
      )

      val fileArray15 = Array(
        FileEntry("/test/path151", 1500, 15),
        FileEntry("/test/path152", 1500, 15),
      )

      val fileArray35 = Array(
        FileEntry("/test/path351", 3500, 35),
        FileEntry("/test/path352", 3500, 35),
      )

      val fileArray50 = Array(
        FileEntry("/test/path501", 8000, 50),
        FileEntry("/test/path502", 8000, 50),
      )


      testS3SourceLog.add(1, fileArray1, Some(300))
      testS3SourceLog.add(2, fileArray2, Some(300))
      testS3SourceLog.add(6, fileArray6, Some(600))

      // move lastPurgeTimestamp
      fileCache.add(
        "/test/path151",
        QueueMessageDesc(1500, isProcessed = false, Some("id151"))
      )
      fileCache.purge()

      testS3SourceLog.add(15, fileArray15, Some(5000))
      testS3SourceLog.add(35, fileArray35, Some(5000))


      testS3SourceLog.get(1) shouldEqual None
      testS3SourceLog.get(2) shouldEqual None
      testS3SourceLog.get(6).get shouldEqual fileArray6
      testS3SourceLog.get(15).get shouldEqual fileArray15
      testS3SourceLog.get(35).get shouldEqual fileArray35

      testS3SourceLog.getFile("/test/path11") shouldEqual None
      testS3SourceLog.getFile("/test/path12") shouldEqual None
      testS3SourceLog.getFile("/test/path21") shouldEqual None
      testS3SourceLog.getFile("/test/path22") shouldEqual None
      testS3SourceLog.getFile("/test/path61") shouldEqual Some(300)
      testS3SourceLog.getFile("/test/path62") shouldEqual Some(600)
      testS3SourceLog.getFile("/test/path151") shouldEqual Some(1500)
      testS3SourceLog.getFile("/test/path152") shouldEqual Some(1500)
      testS3SourceLog.getFile("/test/path351") shouldEqual Some(3500)
      testS3SourceLog.getFile("/test/path352") shouldEqual Some(3500)

      // move lastPurgeTimestamp
      fileCache.add(
        "/test/path501",
        QueueMessageDesc(8000, isProcessed = false, Some("id501"))
      )
      fileCache.purge()

      testS3SourceLog.add(50, fileArray50, Some(9000))

      testS3SourceLog.get(1) shouldEqual None
      testS3SourceLog.get(2) shouldEqual None
      testS3SourceLog.get(6) shouldEqual None
      testS3SourceLog.get(15) shouldEqual None
      testS3SourceLog.get(35) shouldEqual None
      testS3SourceLog.get(50).get shouldEqual fileArray50

      testS3SourceLog.getFile("/test/path11") shouldEqual None
      testS3SourceLog.getFile("/test/path12") shouldEqual None
      testS3SourceLog.getFile("/test/path21") shouldEqual None
      testS3SourceLog.getFile("/test/path22") shouldEqual None
      testS3SourceLog.getFile("/test/path61") shouldEqual None
      testS3SourceLog.getFile("/test/path62") shouldEqual None
      testS3SourceLog.getFile("/test/path151") shouldEqual None
      testS3SourceLog.getFile("/test/path152") shouldEqual None
      testS3SourceLog.getFile("/test/path351") shouldEqual None
      testS3SourceLog.getFile("/test/path352") shouldEqual None
      testS3SourceLog.getFile("/test/path501") shouldEqual Some(8000)
      testS3SourceLog.getFile("/test/path502") shouldEqual Some(8000)
    }
  }
}
