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

package it.spark.sql.streaming.connector.client

import java.util.concurrent.CopyOnWriteArrayList

import scala.language.implicitConversions

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.spark.sql.streaming.connector.{FileMetadata, S3ConnectorSourceOptions}
import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions.{QUEUE_REGION, QUEUE_URL, S3_FILE_FORMAT}
import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import com.amazonaws.spark.sql.streaming.connector.client.{AsyncQueueClient, AsyncSqsClientBuilder}
import it.spark.sql.streaming.connector.{IntegrationTestSuite, S3ConnectorItBase, SqsTest}
import it.spark.sql.streaming.connector.ItTestUtils._
import it.spark.sql.streaming.connector.SqsTest.SLEEP_TIME_MID
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

@IntegrationTestSuite
class AsyncSqsClientItSuite extends S3ConnectorItBase with SqsTest {

  override val uploadS3Path: String = getUploadS3Path
  override val testRegion: String = getTestRegion
  override val testClient: AmazonSQS = getClient(testRegion)
  override val testQueueUrl: String = getQueueUrl

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("INFO")

    purgeQueue(testQueueUrl)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    waitForQueueReady(testQueueUrl, 0)
  }

  override def afterAll(): Unit = {

    try {
      Thread.sleep(SLEEP_TIME_MID)
      waitForQueueReady(testQueueUrl, 0)
      super.afterAll()
    } finally {
      removeTempS3FolderIfEnabled(uploadS3Path, testRegion)
      purgeQueue(testQueueUrl)
    }
  }

  override def getDefaultOptions(fileFormat: String): Map[String, String] = {
    Map(
      S3_FILE_FORMAT -> fileFormat,
      QUEUE_URL -> testQueueUrl,
      QUEUE_REGION -> testRegion
    )
  }

  val DEFAULT_TEST_FILE_FORMAT = "csv"

  test("read message from SQS") {
    withTestTempDir { tempDir =>
      val testDataPath: String = createTestCSVFiles(tempDir)

      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, DEFAULT_TEST_FILE_FORMAT, testRegion)

      var asyncClient: AsyncQueueClient[String] = null
      val pathList = new CopyOnWriteArrayList[String]

      try {
        val sourceOptions = S3ConnectorSourceOptions(getDefaultOptions(DEFAULT_TEST_FILE_FORMAT))
        asyncClient = new AsyncSqsClientBuilder().sourceOptions(
          sourceOptions
        )
        .consumer(
          (data: FileMetadata[String]) => {
            logInfo(s"consumer data: ${data}")
            data.messageId.map(asyncClient.handleProcessedMessage)
            pathList.add(data.filePath)
          }
        )
        .build()

        asyncClient.asyncFetch(sourceOptions.queueFetchWaitTimeoutSeconds)
      } finally {
        if (asyncClient != null) {
          asyncClient.close()
        }
      }

      asyncClient.metrics.fetchThreadUncaughtExceptionCounter.getCount shouldBe 0
      var part0Cnt = 0
      var part1Cnt = 0
      pathList.forEach(
        path => {
          if (path.contains("part-00000")) part0Cnt = part0Cnt + 1
          else if (path.contains("part-00001")) part1Cnt = part1Cnt + 1
        }
      )

      pathList.size shouldBe 2
      part0Cnt shouldBe 1
      part1Cnt shouldBe 1

      waitForQueueReady(testQueueUrl, 0)
    }

  }

  test("read message from SQS with consumer exception") {
    withTestTempDir { tempDir =>
      val testDataPath: String = createTestCSVFiles(tempDir)


      recursiveUploadNewFilesToS3(uploadS3Path, testDataPath, DEFAULT_TEST_FILE_FORMAT, testRegion)

      var asyncClient: AsyncQueueClient[String] = null
      val pathList = new CopyOnWriteArrayList[String]

      try {
        val sourceOptions = S3ConnectorSourceOptions(getDefaultOptions(DEFAULT_TEST_FILE_FORMAT))
        asyncClient = new AsyncSqsClientBuilder().sourceOptions(
          sourceOptions
        )
        .consumer(
          (data: FileMetadata[String]) => {
            logInfo(s"consumer data: ${data}")
            try{
              val path = data.filePath
              if (path.contains("part-00001")) {
                throw new RuntimeException("exception in consumer")
              }

              pathList.add(path)
            }
            finally {
              data.messageId.map(asyncClient.deleteInvalidMessageIfNecessary)
            }

          }
        )
        .build()

        asyncClient.asyncFetch(sourceOptions.queueFetchWaitTimeoutSeconds)
      } finally {
        if (asyncClient != null) {
          asyncClient.close()
        }
      }

      asyncClient.metrics.fetchThreadUncaughtExceptionCounter.getCount shouldBe 0

      pathList.size shouldBe 1
      pathList.get(0) should include ("part-00000")

      waitForQueueReady(testQueueUrl, 0)
    }

  }

}