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
package it.spark.sql.streaming.connector

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.spark.sql.streaming.connector.{S3ConnectorReprocessDryRunException, S3ConnectorReprocessLockExistsException}
import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions._
import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import it.spark.sql.streaming.connector.ItTestUtils._
import it.spark.sql.streaming.connector.SqsTest.SLEEP_TIME_MID
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, Trigger}
import org.apache.spark.sql.types._

abstract class S3ConnectorSourceIntegrationTest extends S3ConnectorItBase
  with QueueTestBase
  with Matchers {

  override val testRegion: String = getTestRegion
  override val uploadS3Path: String = getUploadS3Path
  override val testQueueUrl: String = getQueueUrl

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("INFO")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    waitForQueueReady(testQueueUrl, 0)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      Thread.sleep(SLEEP_TIME_MID)
      waitForQueueReady(testQueueUrl, 0)
    } finally {
      removeTempS3FolderIfEnabled(uploadS3Path, testRegion)
    }

  }

  test("New S3 CSV files loaded to memory") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawData: _*),
        StopStream
      )
    }
  }

  test("New S3 CSV files loaded to memory trigger once") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      testStream(inputDf)(
        StartStream(Trigger.Once()),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawData: _*),
        StopStream
      )
    }
  }

  test("New S3 CSV files loaded to memory with header option") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val SEP = "|"
      val testDataPath: String = createTestCSVFilesWithHeader(tempDir, SEP)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT) + ("header" -> "true", "sep" -> SEP))
        .load()

      val query = inputDf
        .select("testString", "testInt")
        .where("testInt==5000")

      testStream(query)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(
          Row("Michael", 5000),
          Row("Robert", 5000)
        ),
        StopStream
      )
    }
  }

  test("New S3 CSV files loaded to memory based on maxFilePerTrigger") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val queryName: String = "s3connetorquery"
      val testDataPath: String = createTestCSVFiles(tempDir)
      val checkpointDir = s"${tempDir}/checkpoint/"
      val LOOP_COUNT = 10

      (0 until LOOP_COUNT).foreach { i =>
        val dest = s"${uploadS3Path}${i}/"
        recursiveUploadNewFilesToS3(dest, testDataPath, TEST_FILE_FORMAT, testRegion)
      }

      waitForQueueReady(testQueueUrl, LOOP_COUNT*TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      val query = inputDf
        .writeStream
        .queryName(queryName)
        .format("memory")
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .start()

      waitForQueryStarted(query)
      query.processAllAvailable()


      val minBatchCount: Long = (LOOP_COUNT*TEST_SPARK_PARTITION)/TEST_MAX_FILES_PER_TRIGGER
      spark.table(queryName).count() shouldEqual  (LOOP_COUNT*testRawData.length).toLong
      query.lastProgress.batchId + 1 should be >= minBatchCount
      logInfo(s"query.lastProgress.batchId: ${query.lastProgress.batchId}")
      waitForQueueReady(testQueueUrl, 0)
      query.stop()
    }
  }

  test("New S3 CSV files loaded to memory based on pathGlobFilter") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path + "filter1/", testDataPath, TEST_FILE_FORMAT, testRegion)
      recursiveUploadNewFilesToS3(uploadS3Path + "filter2/", testDataPath, TEST_FILE_FORMAT, testRegion)
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION*2)

      val options = getDefaultOptions(TEST_FILE_FORMAT) + (PATH_GLOB_FILTER -> "*/filter1/*.csv")

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(options)
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawData: _*),
        StopStream
      )
    }
  }

  test("New S3 file with partitions loaded to memory") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFilesWithPartition(tempDir)

      val options = getDefaultOptions(TEST_FILE_FORMAT) + (
        PARTITION_COLUMNS -> "testPart1,testPart2",
        BASE_PATH -> uploadS3Path
        )

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)

      waitForQueueReady(testQueueUrl, 2*TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchemaWithPartition)
        .options(options)
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawDataWithPartition: _*),
        StopStream
      )
    }
  }

  test("Load schema mismatch CSV files from S3") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)

      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val newTestSchema: StructType = StructType(Array(
        StructField("testInt", IntegerType, nullable = true)
      ))

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(newTestSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(
          Row(null),
          Row(null),
          Row(null)),
        StopStream
      )
    }
  }

  test("Load schema mismatch CSV files from S3 with DROPMALFORMED mode") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)

      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val newTestSchema: StructType = StructType(Array(
        StructField("testInt", IntegerType, nullable = true)
      ))

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(newTestSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT) + ("mode" -> "DROPMALFORMED") )
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(),
        StopStream
      )
    }
  }

  test("New S3 parquet files loaded to memory") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "parquet"
      val testDataPath: String = createTestParquetFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)

      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawData: _*),
        StopStream
      )
    }
  }

  test("Load schema mismatch parquet files from S3") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "parquet"

      val testDataPath: String = createTestParquetFiles(tempDir)

      val newTestSchema: StructType = StructType(Array(
        StructField("newtestInt", IntegerType, nullable = true),
        StructField("testString", StringType, nullable = true)
      ))

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, "", testRegion) // Upload all files
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION * 2 + 2)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(newTestSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(
          Row(null, "James"),
          Row(null, "Michael"),
          Row(null, "Robert")),
        StopStream
      )
    }
  }

  test("New S3 json files loaded to memory") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "json"
      val testDataPath: String = createTestJsonFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawData: _*),
        StopStream
      )
    }
  }

  test("Duplicate S3 files ignored") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val options = getDefaultOptions(TEST_FILE_FORMAT)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(options)
        .load()

      testStream(inputDf)(
        StartStream(),
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawData: _*),
        AssertOnQuery { q =>
          // upload duplicated files
          recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)
          Thread.sleep(SLEEP_TIME_MID)
          q.processAllAvailable()
          true
        },
        CheckAnswer(testRawData: _*),
        AssertOnQuery { q =>
          // upload files to new path
          recursiveUploadNewFilesToS3(uploadS3Path + "nodup/", testDataPath, TEST_FILE_FORMAT, testRegion)
          Thread.sleep(SLEEP_TIME_MID)
          q.processAllAvailable()
          true
        },
        CheckAnswer((testRawData ++ testRawData): _*),
        StopStream
      )
    }
  }

  test("reprocess previous batches") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val queryName: String = "reprocess"
      val testDataPath: String = createTestCSVFiles(tempDir)
      val checkpointDir = s"${tempDir}/checkpoint/"
      val LOOP_COUNT = 10

      val FOR_EACH_WRITER_KEY1 = "reprocess_foreachwriter1"
      val FOR_EACH_WRITER_KEY2 = "reprocess_foreachwriter2"
      val FOR_EACH_WRITER_KEY3 = "reprocess_foreachwriter3"

      (0 until LOOP_COUNT).foreach { i =>
        val dest = s"${uploadS3Path}${i}/"
        recursiveUploadNewFilesToS3(dest, testDataPath, TEST_FILE_FORMAT, testRegion)
      }

      waitForQueueReady(testQueueUrl, LOOP_COUNT*TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      val query = inputDf
        .writeStream
        .queryName(queryName)
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .trigger(Trigger.ProcessingTime(SLEEP_TIME_MID.milliseconds))
        .foreach(new TestForeachWriter[Row](FOR_EACH_WRITER_KEY1, r => r.mkString))
        .start()

      waitForQueryStarted(query)
      query.processAllAvailable()
      query.stop()

      val lastBatchId = if (query.lastProgress.numInputRows == 0) query.lastProgress.batchId - 1
                        else query.lastProgress.batchId

      val inputDf2 = spark
      .readStream
      .format(SOURCE_SHORT_NAME)
      .schema(testSchema)
      .options(getDefaultOptions(TEST_FILE_FORMAT) +
        (REPROCESS_START_BATCH_ID -> "0",
          REPROCESS_END_BATCH_ID -> s"${lastBatchId}",
          REPROCESS_DRY_RUN -> "false"
        ))
      .load()

      val query2 = inputDf2
        .writeStream
        .queryName(queryName)
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .foreach(new TestForeachWriter[Row](FOR_EACH_WRITER_KEY2, r => r.mkString))
        .start()


      waitForQueryStarted(query2)
      query2.processAllAvailable()

      query2.stop()

      val lastBatchId2 = if (query2.lastProgress.numInputRows == 0) query2.lastProgress.batchId - 1
                         else query2.lastProgress.batchId

      lastBatchId2 should be >= 2 * lastBatchId
      val result2 = TestForeachWriter.getValues(FOR_EACH_WRITER_KEY2)
      assert(result2.length === LOOP_COUNT * testRawData.size, s"Unexpected results: ${result2.toList}")

      val inputDf3 = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT) +
          (REPROCESS_START_BATCH_ID -> "0",
            REPROCESS_END_BATCH_ID -> s"${lastBatchId}",
            REPROCESS_DRY_RUN -> "false"
          ))
        .load()

      val query3 = inputDf3
        .writeStream
        .queryName(queryName)
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .trigger(Trigger.ProcessingTime(SLEEP_TIME_MID.milliseconds))
        .foreach(new TestForeachWriter[Row](FOR_EACH_WRITER_KEY3, r => r.mkString))
        .start()

      val exception = intercept[StreamingQueryException] {
        waitForQueryStarted(query3)
        query3.processAllAvailable()
      }

      exception.cause.isInstanceOf[S3ConnectorReprocessLockExistsException] shouldBe true
      query3.stop()

    }
  }

  test("reprocess previous batches dry run") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val queryName: String = "reprocessDryRun"
      val testDataPath: String = createTestCSVFiles(tempDir)
      val checkpointDir = s"${tempDir}/checkpoint/"
      val LOOP_COUNT = 10

      val FOR_EACH_WRITER_KEY1 = "reprocessdryrun_foreachwriter1"
      val FOR_EACH_WRITER_KEY2 = "reprocessdryrun_foreachwriter2"

      (0 until LOOP_COUNT).foreach { i =>
        val dest = s"${uploadS3Path}${i}/"
        recursiveUploadNewFilesToS3(dest, testDataPath, TEST_FILE_FORMAT, testRegion)
      }

      waitForQueueReady(testQueueUrl, LOOP_COUNT*TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      val query = inputDf
        .writeStream
        .queryName(queryName)
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .trigger(Trigger.ProcessingTime(SLEEP_TIME_MID.milliseconds))
        .foreach(new TestForeachWriter[Row](FOR_EACH_WRITER_KEY1, r => r.mkString))
        .start()

      waitForQueryStarted(query)
      query.processAllAvailable()
      query.stop()

      val lastBatchId = if (query.lastProgress.numInputRows == 0) query.lastProgress.batchId - 1
      else query.lastProgress.batchId

      val inputDf2 = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT) +
          (REPROCESS_START_BATCH_ID -> "0",
            REPROCESS_END_BATCH_ID -> s"${lastBatchId}",
            REPROCESS_DRY_RUN -> "true"
          ))
        .load()

      val query2 = inputDf2
        .writeStream
        .queryName(queryName)
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .foreach(new TestForeachWriter[Row](FOR_EACH_WRITER_KEY2, r => r.mkString))
        .start()

      val exception = intercept[StreamingQueryException] {
        waitForQueryStarted(query2)
        query2.processAllAvailable()
      }

      exception.cause.isInstanceOf[S3ConnectorReprocessDryRunException] shouldBe true
      exception.getMessage should include(s"${lastBatchId +1} batches, ${LOOP_COUNT*TEST_SPARK_PARTITION} files.")

      query2.stop()

    }
  }


  // This test case stops spark session so keep it as the last one
  test("Restore from existing checkpoint files") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)
      val checkpointDir = s"${tempDir}/checkpoint/"

      val queryName1: String = "restores3connetorquerypart1"
      val queryName2: String = "restores3connetorquerypart2"
      val FOR_EACH_WRITER_KEY1 = "restore_foreachwriter1"
      val FOR_EACH_WRITER_KEY2 = "restore_foreachwriter2"

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)

      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      val query = inputDf
        .writeStream
        .queryName(queryName1)
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .foreach(new TestForeachWriter[Row](FOR_EACH_WRITER_KEY1, r => r.mkString))
        .start()

      waitForQueryStarted(query)
      query.processAllAvailable()

      query.stop()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      val result1 = TestForeachWriter.getValues(FOR_EACH_WRITER_KEY1)
      assert(result1.length === 3, s"Unexpected results: ${result1.toList}")
      assert(
        result1.toSet ===
        Seq(Row("James", true, 3000).mkString,
        Row("Michael", false, 5000).mkString,
        Row("Robert", false, 5000).mkString).toSet
      )

      val spark2 = SparkSession.builder()
        .master("local[2]")
        .appName("RestoreCheckpointTestPart2")
        .config("spark.sql.ui.explainMode", "extended")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
          "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()

      // Start the streaming again and restore from the same checkpoint
      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, TEST_FILE_FORMAT, testRegion)
      waitForQueueReady(testQueueUrl, TEST_SPARK_PARTITION)

      val inputDf2 = spark2
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchema)
        .options(getDefaultOptions(TEST_FILE_FORMAT))
        .load()

      val query2 = inputDf2
        .writeStream
        .queryName(queryName2)
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", checkpointDir)
        .foreach(new TestForeachWriter[Row](FOR_EACH_WRITER_KEY2, r => r.mkString))
        .start()

      waitForQueryStarted(query2)
      query2.processAllAvailable()

      val result2 = TestForeachWriter.getValues(FOR_EACH_WRITER_KEY2)
      assert(result2.length === 0, s"Unexpected results: ${result2.toList}")

      query2.stop()
      spark2.stop()
    }
  }

}

@IntegrationTestSuite
class S3ConnectorSourceSqsRocksDBItSuite  extends S3ConnectorSourceIntegrationTest with SqsTest {

  override val testClient: AmazonSQS = getClient(testRegion)

  override def beforeAll(): Unit = {
    super.beforeAll()
    purgeQueue(testQueueUrl)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    purgeQueue(testQueueUrl)
  }

  override def getDefaultOptions(fileFormat: String): Map[String, String] = {
    Map(
      QUEUE_REGION -> testRegion,
      S3_FILE_FORMAT -> fileFormat,
      MAX_FILES_PER_TRIGGER -> TEST_MAX_FILES_PER_TRIGGER.toString,
      QUEUE_URL -> testQueueUrl,
      SQS_LONG_POLLING_WAIT_TIME_SECONDS -> "15",
      SQS_VISIBILITY_TIMEOUT_SECONDS -> "20"
    )
  }
}

