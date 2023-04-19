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

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.sqs.model.{GetQueueAttributesRequest, PurgeQueueRequest}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import it.spark.sql.streaming.connector.SqsTest.{PURGE_RETRY_INTERVAL_MS, PURGE_WAIT_TIME_SECONDS, RETRY_INTERVAL_MS, SLEEP_TIME_MID, WAIT_TIME_SECONDS}
import org.apache.spark.internal.Logging
import org.scalatest.concurrent.Eventually.{eventually, interval}
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.util.{Failure, Try}

trait QueueTestBase {
  def testQueueUrl: String

  protected def purgeQueue(url: String): Unit
  protected def waitForQueueReady(url: String, msgCount: Int): Unit
}

trait QueueTestClientBase[T] {
  def testClient: T
  protected def getClient(region: String): T
}

trait SqsTest extends QueueTestBase with QueueTestClientBase[AmazonSQS] with Logging {

  override def getClient(region: String): AmazonSQS = {
    AmazonSQSClientBuilder
      .standard()
      .withClientConfiguration(new ClientConfiguration().withMaxConnections(1))
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .withRegion(region)
      .build()
  }

  override def purgeQueue(url: String): Unit = {

    val purgeRequest = new PurgeQueueRequest(url)
    Thread.sleep(SLEEP_TIME_MID)

    eventually(timeout(PURGE_WAIT_TIME_SECONDS.seconds),
      interval((PURGE_RETRY_INTERVAL_MS*10).milliseconds)) {
        Try(testClient.purgeQueue(purgeRequest)) match {
          case Failure(e) =>
            logInfo("purgeQueue failed", e)
            throw e
          case _ =>
            Thread.sleep(SLEEP_TIME_MID)
            logInfo("purgeQueue success. Checking queue status.")
            waitForQueueReady(url, 0)
        }
      }
  }

  override def waitForQueueReady(url: String, msgCount: Int): Unit = {
    eventually(timeout(WAIT_TIME_SECONDS.seconds), interval((RETRY_INTERVAL_MS*10).milliseconds)) {
      val attrs = testClient.getQueueAttributes(new GetQueueAttributesRequest(url)
        .withAttributeNames("ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible"))
      val approximateNumberOfMessages = attrs.getAttributes.get("ApproximateNumberOfMessages").toInt
      val approximateNumberOfMessagesNotVisible = attrs.getAttributes.get("ApproximateNumberOfMessagesNotVisible").toInt
      assert( approximateNumberOfMessages == msgCount,
        s"ApproximateNumberOfMessages is ${approximateNumberOfMessages}, expected: ${msgCount}")
      assert( approximateNumberOfMessagesNotVisible == 0,
        s"ApproximateNumberOfMessagesNotVisible is ${approximateNumberOfMessagesNotVisible}, expected 0")
    }
  }
}

object SqsTest {
  val SLEEP_TIME_SHORT = 5000
  val SLEEP_TIME_MID = 10000

  val WAIT_TIME_SECONDS = 60
  val RETRY_INTERVAL_MS = 500

  val PURGE_WAIT_TIME_SECONDS = WAIT_TIME_SECONDS * 3 // must > 60, as SQS purge only allow once per minute
  val PURGE_RETRY_INTERVAL_MS = 5000
}