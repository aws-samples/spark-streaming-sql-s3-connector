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

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions.{MAX_FILES_PER_TRIGGER, QUEUE_REGION, QUEUE_URL, REPROCESS_DRY_RUN, REPROCESS_END_BATCH_ID, REPROCESS_START_BATCH_ID, S3_FILE_FORMAT}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.network.util.JavaUtils

class S3ConnectorSourceOptionsSuite extends S3ConnectorTestBase  {
    test("option uses default values") {
        val option = S3ConnectorSourceOptions( Map(
            S3_FILE_FORMAT -> TESTBASE_DEFAULT_FILE_FORMAT,
            QUEUE_URL -> TESTBASE_DEFAULT_QUEUE_URL,
            QUEUE_REGION -> TESTBASE_DEFAULT_QUEUE_REGION
        ))

        option.fileFormat shouldBe TESTBASE_DEFAULT_FILE_FORMAT
        option.queueUrl shouldBe TESTBASE_DEFAULT_QUEUE_URL
        option.queueRegion shouldBe TESTBASE_DEFAULT_QUEUE_REGION
        option.queueType shouldBe S3ConnectorSourceOptions.SQS_QUEUE
        option.queueFetchWaitTimeoutSeconds shouldBe 2 * option.sqsLongPollWaitTimeSeconds
        option.pathGlobFilter shouldBe None
        option.partitionColumns shouldBe None
        option.maxFileAgeMs shouldBe JavaUtils.timeStringAsMs(S3ConnectorSourceOptions.MAX_FILE_AGE_DEFAULT_VALUE)
        option.maxFilesPerTrigger shouldBe Some(S3ConnectorSourceOptions.MAX_FILES_PER_TRIGGER_DEFAULT_VALUE)
        option.reprocessStartBatchId shouldBe None
        option.reprocessEndBatchId shouldBe None
        option.reprocessState shouldBe ReprocessStates.NoReprocess
        option.sqsMaxRetries shouldBe S3ConnectorSourceOptions.SQS_MAX_RETRIES_DEFAULT_VALUE
        option.sqsMaxConcurrency shouldBe S3ConnectorSourceOptions.SQS_MAX_CONCURRENCY_DEFAULT_VALUE
        option.sqsLongPollWaitTimeSeconds shouldBe
          S3ConnectorSourceOptions.SQS_LONG_POLLING_WAIT_TIME_SECONDS_DEFAULT_VALUE
        option.sqsVisibilityTimeoutSeconds shouldBe S3ConnectorSourceOptions.SQS_VISIBILITY_TIMEOUT_DEFAULT_VALUE
        option.sqsKeepMessageForConsumerError shouldBe
          S3ConnectorSourceOptions.SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR_DEFAULT_VALUE

    }

    test("maxFilesPerTrigger given a value") {
        val option = S3ConnectorSourceOptions(defaultOptionMap + (MAX_FILES_PER_TRIGGER -> "50"))
        option.maxFilesPerTrigger shouldBe Some(50)
    }

    test("maxFilesPerTrigger set to -1") {
        val option = S3ConnectorSourceOptions(defaultOptionMap + (MAX_FILES_PER_TRIGGER -> "-1"))
        option.maxFilesPerTrigger shouldBe None
    }

    test("only reprocessStartBatchId set") {
        val option = S3ConnectorSourceOptions(defaultOptionMap
          + (REPROCESS_START_BATCH_ID -> "50")
        )
        option.reprocessState shouldBe ReprocessStates.NoReprocess
    }

    test("only reprocessEndBatchId set") {
        val option = S3ConnectorSourceOptions(defaultOptionMap
          + (REPROCESS_END_BATCH_ID -> "50")
        )
        option.reprocessState shouldBe ReprocessStates.NoReprocess
    }

    test("Both reprocessStartBatchId and reprocessEndBatchId set") {
        val option = S3ConnectorSourceOptions(defaultOptionMap
          + (REPROCESS_START_BATCH_ID -> "50",
          REPROCESS_END_BATCH_ID -> "60")
        )
        option.reprocessState shouldBe ReprocessStates.DryRun
    }

    test("Both reprocessDryRun set") {
        val option = S3ConnectorSourceOptions(defaultOptionMap
          + (REPROCESS_START_BATCH_ID -> "50",
          REPROCESS_END_BATCH_ID -> "60",
          REPROCESS_DRY_RUN -> "false"
        )
        )
        option.reprocessState shouldBe ReprocessStates.InAction
    }

    test("reprocessStartBatchId larger than reprocessEndBatchId") {

      val exception = intercept[IllegalArgumentException] {
          S3ConnectorSourceOptions(defaultOptionMap
            + (REPROCESS_START_BATCH_ID -> "70",
            REPROCESS_END_BATCH_ID -> "60")
          )
      }

      exception.getMessage should include("reprocessStartBatchId must be less than or equal to reprocessEndBatchId")
    }

}
