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

import com.amazonaws.spark.sql.streaming.connector.client.{AsyncClientMetrics, AsyncQueueClient}
import com.codahale.metrics.Counter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.connector.s3.S3SparkUtils
import org.mockito.stubbing.Stubber
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.io.{File, IOException}
import java.util.UUID

object TestUtils extends Logging {

  // Set this parameter to false to keep files in local temp dir
  val TEMP_DIR_CLEAN_UP: Boolean = false

  def doReturnMock(value: Any): Stubber = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  // scalastyle:off argcount
  def verifyMetrics(metrics: AsyncClientMetrics,
      expectedReceiveMessageCount: Int = 0,
      expectedReceiveMessageFailedCount: Int = 0,
      expectedParseMessageCount: Int = 0,
      expectedParseMessageFailedCount: Int = 0,
      expectedDiscardedMessageCount: Int = 0,
      expectedConsumeMessageCount: Int = 0,
      expectedConsumeMessageFailedCount: Int = 0,
      expectedDeleteMessageCount: Int = 0,
      expectedDeleteMessageFailedCount: Int = 0,
      expectedSetMessageVisibilityCount: Int = 0,
      expectedSetMessageVisibilityFailedCount: Int = 0,
      expectedFetchThreadConsumeMessageCount: Int = 0,
      expectedFetchThreadConsumeMessageFailedCount: Int = 0,
      expectedFetchThreadUncaughtExceptionCount: Int = 0
   ): Unit = {

    metrics.receiveMessageCounter.getCount shouldBe expectedReceiveMessageCount
    metrics.receiveMessageFailedCounter.getCount shouldBe expectedReceiveMessageFailedCount
    metrics.parseMessageCounter.getCount shouldBe expectedParseMessageCount
    metrics.parseMessageFailedCounter.getCount shouldBe expectedParseMessageFailedCount
    metrics.discardedMessageCounter.getCount shouldBe expectedDiscardedMessageCount
    metrics.consumeMessageCounter.getCount shouldBe expectedConsumeMessageCount
    metrics.consumeMessageFailedCounter.getCount shouldBe expectedConsumeMessageFailedCount
    metrics.deleteMessageCounter.getCount shouldBe expectedDeleteMessageCount
    metrics.deleteMessageFailedCounter.getCount shouldBe expectedDeleteMessageFailedCount
    metrics.setMessageVisibilityCounter.getCount shouldBe expectedSetMessageVisibilityCount
    metrics.setMessageVisibilityFailedCounter.getCount shouldBe expectedSetMessageVisibilityFailedCount
    metrics.fetchThreadConsumeMessageCounter.getCount shouldBe expectedFetchThreadConsumeMessageCount
    metrics.fetchThreadConsumeMessageFailedCounter.getCount shouldBe expectedFetchThreadConsumeMessageFailedCount
    metrics.fetchThreadUncaughtExceptionCounter.getCount shouldBe expectedFetchThreadUncaughtExceptionCount

  }
  // scalastyle:on argcount

  def withTestTempDir(f: File => Unit): Unit = {
    val dir = createDirectory("testDir", "S3-connector")
    try f(dir) finally {
      if (TEMP_DIR_CLEAN_UP) S3SparkUtils.deleteRecursively(dir)
    }
  }

  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 15
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case e: SecurityException => dir = null;
      }
    }

    dir.getCanonicalFile
  }

  def recursiveList(f: File): Array[File] = {
    require(f.isDirectory)
    val current = f.listFiles
    current ++ current.filter(_.isDirectory).flatMap(recursiveList)
  }
}
