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

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions._
import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import it.spark.sql.streaming.connector.ItTestUtils.{getCrossAccountQueueUrl, getCrossAccountTestRegion, getCrossAccountUploadS3Path, recursiveUploadNewFilesToS3, removeTempS3FolderIfEnabled}
import it.spark.sql.streaming.connector.SqsTest.SLEEP_TIME_MID
import org.scalatest.matchers.must.Matchers

abstract class S3ConnectorSourceCrossAccountIntegrationTest extends S3ConnectorItBase
  with QueueTestBase
  with Matchers {

  override val testQueueUrl: String = getCrossAccountQueueUrl
  override val uploadS3Path: String = getCrossAccountUploadS3Path
  override val testRegion: String = getCrossAccountTestRegion

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

  test("Cross account and region new S3 CSV files loaded to memory") {
    withTestTempDir { tempDir =>
      val TEST_FILE_FORMAT = "csv"
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path,
        testDataPath,
        TEST_FILE_FORMAT,
        getCrossAccountTestRegion)

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
}

@IntegrationTestSuite
class S3ConnectorSourceCrossAccountSqsRocksDBItSuite extends S3ConnectorSourceCrossAccountIntegrationTest
  with SqsTest {

  override val testClient = getClient(testRegion)

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
      SQS_VISIBILITY_TIMEOUT_SECONDS -> "120"
    )
  }
}