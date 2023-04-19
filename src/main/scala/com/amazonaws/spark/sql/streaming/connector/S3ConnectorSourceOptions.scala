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

import scala.util.Try

import com.amazonaws.spark.sql.streaming.connector.ReprocessStates.ReprocessState

import org.apache.spark.network.util.JavaUtils

case class S3ConnectorSourceOptions (
  maxFilesPerTrigger: Option[Int],
  pathGlobFilter: Option[String],
  fileFormat: String,
  maxFileAgeMs: Long,
  partitionColumns: Option[String],
  queueRegion: String,
  queueUrl: String,
  queueType: String,
  queueFetchWaitTimeoutSeconds: Long,

  // reprocess
  reprocessStartBatchId: Option[Int], // inclusive
  reprocessEndBatchId: Option[Int], // inclusive
  reprocessState: ReprocessState,

  // SQS parameters
  sqsMaxRetries: Int,
  sqsMaxConcurrency: Int,
  sqsLongPollWaitTimeSeconds: Int,
  sqsVisibilityTimeoutSeconds: Int,
  sqsKeepMessageForConsumerError: Boolean,
)

object S3ConnectorSourceOptions {
  val SQS_QUEUE = "SQS"

  private val PREFIX = "spark.s3conn."
  private val SQS_PREFIX = PREFIX + ".sqs."

  val BASE_PATH: String = "basePath"

  val MAX_FILES_PER_TRIGGER: String = PREFIX + "maxFilesPerTrigger"
  val PATH_GLOB_FILTER: String = PREFIX + "pathGlobFilter"
  val S3_FILE_FORMAT: String = PREFIX + "fileFormat"
  val MAX_FILE_AGE: String = PREFIX + "maxFileAge"
  val PARTITION_COLUMNS: String = PREFIX + "partitionColumns"
  val QUEUE_URL: String = PREFIX + "queueUrl"
  val QUEUE_REGION: String = PREFIX + "queueRegion"
  val QUEUE_TYPE: String = PREFIX + "queueType"
  val QUEUE_FETCH_WAIT_TIMEOUT_SECONDS: String = PREFIX + "queueFetchWaitTimeoutSeconds"

  val REPROCESS_START_BATCH_ID: String = PREFIX + "reprocessStartBatchId"
  val REPROCESS_END_BATCH_ID: String = PREFIX + "reprocessEndBatchId"
  val REPROCESS_DRY_RUN: String = PREFIX + "reprocessDryRun"

  val SQS_LONG_POLLING_WAIT_TIME_SECONDS: String = SQS_PREFIX + "longPollingWaitTimeSeconds"
  val SQS_VISIBILITY_TIMEOUT_SECONDS: String = SQS_PREFIX + "visibilityTimeoutSeconds"
  val SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR: String = SQS_PREFIX + "keepMessageForConsumerError"
  val SQS_MAX_RETRIES: String = SQS_PREFIX + "maxRetries"
  val SQS_MAX_CONCURRENCY: String = SQS_PREFIX + "maxConcurrency"

  val MAX_FILES_PER_TRIGGER_DEFAULT_VALUE: Int = 100
  val MAX_FILE_AGE_DEFAULT_VALUE: String = "15d"
  val REPROCESS_DRY_RUN_DEFAULT_VALUE: Boolean = true
  val SQS_LONG_POLLING_WAIT_TIME_SECONDS_MIN_VALUE: Int = 0
  val SQS_LONG_POLLING_WAIT_TIME_SECONDS_MAX_VALUE: Int = 20
  val SQS_LONG_POLLING_WAIT_TIME_SECONDS_DEFAULT_VALUE: Int = 10
  val SQS_MAX_RETRIES_DEFAULT_VALUE: Int = 10
  val SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR_DEFAULT_VALUE: Boolean = false
  val SQS_MAX_CONCURRENCY_DEFAULT_VALUE: Int = 50
  val SQS_VISIBILITY_TIMEOUT_DEFAULT_VALUE: Int = 60


  def apply(parameters: Map[String, String]): S3ConnectorSourceOptions = {

    val maxFilesPerTrigger: Option[Int] = parameters.get(MAX_FILES_PER_TRIGGER) match {
      case Some(str) => Try(str.toInt).toOption.filter(_ > 0).orElse(None)
      case None => Some(MAX_FILES_PER_TRIGGER_DEFAULT_VALUE)
    }

    val pathGlobFilter = parameters.get(PATH_GLOB_FILTER)

    val fileFormat = parameters.getOrElse(S3_FILE_FORMAT,
      throw new IllegalArgumentException(s"Specifying ${S3_FILE_FORMAT} is mandatory with s3 connector source"))

    val maxFileAgeMs = JavaUtils.timeStringAsMs(parameters.getOrElse(MAX_FILE_AGE, MAX_FILE_AGE_DEFAULT_VALUE))

    val partitionColumns = parameters.get(PARTITION_COLUMNS)

    val queueUrl: String = parameters.getOrElse(QUEUE_URL,
      throw new IllegalArgumentException(s"${QUEUE_URL} is not specified"))

    val queueRegion: String = parameters.getOrElse(QUEUE_REGION,
      throw new IllegalArgumentException(s"${QUEUE_REGION} is not specified"))

    val queueType: String = parameters.getOrElse(QUEUE_TYPE, SQS_QUEUE)

    val REPROCESS_START_LOG_ID: String = PREFIX + "reprocessStartBatchId"
    val REPROCESS_END_LOG_ID: String = PREFIX + "reprocessEndBatchId"
    val REPROCESS_DRY_RUN: String = PREFIX + "reprocessDryRun"

    val reprocessStartBatchId: Option[Int] = parameters.get(REPROCESS_START_LOG_ID).map { str =>
      Try(str.toInt).toOption.filter(_ >= 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '${REPROCESS_START_LOG_ID}', must be zero or a positive integer")
      }
    }

    val reprocessEndBatchId: Option[Int] = parameters.get(REPROCESS_END_LOG_ID).map { str =>
      Try(str.toInt).toOption.filter(_ >= 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '${REPROCESS_END_LOG_ID}', must be zero or a positive integer")
      }
    }

    val reprocessDryRun: Boolean = withBooleanParameter(parameters, REPROCESS_DRY_RUN,
      REPROCESS_DRY_RUN_DEFAULT_VALUE)


    val reprocessState: ReprocessState = {
      for ( startId <- reprocessStartBatchId;
            endId <- reprocessEndBatchId
            ) yield {
        if (startId > endId) {
          throw new IllegalArgumentException(
            s"reprocessStartBatchId must be less than or equal to reprocessEndBatchId: start ${startId}, end ${endId}")
        }

        if (reprocessDryRun) ReprocessStates.DryRun else ReprocessStates.InAction
      }
    } getOrElse ReprocessStates.NoReprocess

    val sqsKeepMessageForConsumerError: Boolean = withBooleanParameter( parameters,
      SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR, SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR_DEFAULT_VALUE)

    val sqsLongPollWaitTimeSeconds: Int = parameters.get(SQS_LONG_POLLING_WAIT_TIME_SECONDS).map { str =>
      Try(str.toInt).toOption.filter{ x =>
        x >= SQS_LONG_POLLING_WAIT_TIME_SECONDS_MIN_VALUE && x <= SQS_LONG_POLLING_WAIT_TIME_SECONDS_MAX_VALUE
      }
      .getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option ${SQS_LONG_POLLING_WAIT_TIME_SECONDS}," +
            s"must be an integer between ${SQS_LONG_POLLING_WAIT_TIME_SECONDS_MIN_VALUE}" +
            s" and ${SQS_LONG_POLLING_WAIT_TIME_SECONDS_MAX_VALUE}")
      }
    }.getOrElse(SQS_LONG_POLLING_WAIT_TIME_SECONDS_DEFAULT_VALUE)

    val sqsMaxRetries: Int = withPositiveIntegerParameter(parameters, SQS_MAX_RETRIES, SQS_MAX_RETRIES_DEFAULT_VALUE)

    val sqsMaxConcurrency: Int = withPositiveIntegerParameter(parameters, SQS_MAX_CONCURRENCY,
      SQS_MAX_CONCURRENCY_DEFAULT_VALUE)

    val sqsVisibilityTimeoutSeconds: Int = withPositiveIntegerParameter(parameters, SQS_VISIBILITY_TIMEOUT_SECONDS,
      SQS_VISIBILITY_TIMEOUT_DEFAULT_VALUE)

    val queueFetchWaitTimeoutSeconds: Long = withPositiveIntegerParameter(parameters, QUEUE_FETCH_WAIT_TIMEOUT_SECONDS,
      2 * sqsLongPollWaitTimeSeconds)

    new S3ConnectorSourceOptions(
      maxFilesPerTrigger = maxFilesPerTrigger,
      pathGlobFilter = pathGlobFilter,
      fileFormat = fileFormat,
      maxFileAgeMs = maxFileAgeMs,
      partitionColumns = partitionColumns,
      queueRegion = queueRegion,
      queueUrl = queueUrl,
      queueType = queueType,
      queueFetchWaitTimeoutSeconds = queueFetchWaitTimeoutSeconds,
      reprocessStartBatchId = reprocessStartBatchId,
      reprocessEndBatchId = reprocessEndBatchId,
      reprocessState = reprocessState,
      sqsLongPollWaitTimeSeconds = sqsLongPollWaitTimeSeconds,
      sqsMaxRetries = sqsMaxRetries,
      sqsMaxConcurrency = sqsMaxConcurrency,
      sqsVisibilityTimeoutSeconds = sqsVisibilityTimeoutSeconds,
      sqsKeepMessageForConsumerError = sqsKeepMessageForConsumerError
    )
  }

  private def withPositiveIntegerParameter(parameters: Map[String, String], name: String, default: Int) = {
    parameters.get(name).map { str =>
      Try(str.toInt).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '${name}', must be a positive integer")
      }
    }.getOrElse(default)
  }

  private def withBooleanParameter(parameters: Map[String, String], name: String, default: Boolean) = {
    parameters.get(name).map { str =>
      try {
        str.toBoolean
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid value '$str' for option '$name', must be true or false")
      }
    }.getOrElse(default)
  }
}

object ReprocessStates extends Enumeration {
  type ReprocessState = Value
  val NoReprocess, DryRun, InAction = Value
}