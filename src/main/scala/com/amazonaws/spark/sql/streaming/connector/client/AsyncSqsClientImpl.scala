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
package com.amazonaws.spark.sql.streaming.connector.client

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.concurrent._
import java.util.function.{BiConsumer, BiFunction, Consumer}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.util.{Failure, Try}
import scala.util.control.Breaks.{break, breakable}
import scala.util.control.NonFatal

import com.amazonaws.spark.sql.streaming.connector.{FileMetadata, S3ConnectorSourceOptions}
import com.amazonaws.spark.sql.streaming.connector.Utils.{convertTimestampToMills, reportTimeTaken, shutdownAndAwaitTermination}
import com.amazonaws.spark.sql.streaming.connector.client.AsyncQueueConsumerResults.AsyncQueueConsumerResult
import com.amazonaws.spark.sql.streaming.connector.client.AsyncSqsClientImpl.{MAX_POOL_SIZE, MAX_SQS_BATCH_SIZE}
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.connector.s3.S3SparkUtils.{newDaemonFixedThreadPool, newDaemonSingleThreadScheduledExecutor}

class AsyncSqsClientImpl[T] (client: SqsAsyncClient,
                          sourceOptions: S3ConnectorSourceOptions,
                          messageConsumer: Option[Consumer[FileMetadata[T]]]
                         ) extends AsyncQueueClient[T] with Logging {

  override val queueUrl: String = sourceOptions.queueUrl


  val poolSize: Int = math.min(sourceOptions.sqsMaxConcurrency, MAX_POOL_SIZE)

  override val metrics: AsyncClientMetrics = AsyncSqsClientMetricsImpl()

  val consumerExecutor: ThreadPoolExecutor = newDaemonFixedThreadPool(poolSize, "asyncSqsConsumerPool")

  val asyncSqsFetchExecutor: ScheduledExecutorService = newDaemonSingleThreadScheduledExecutor(
    "asyncSqsFetch")

  override def asyncFetch(waitTimeoutSecond: Long): Future[_] = {
    require(messageConsumer.isDefined, "Message consumer must be defined to start asyncFetch")

    val startTime = System.currentTimeMillis()

    def fetchAndConsumeMessage: CompletableFuture[Seq[AsyncQueueConsumerResult]] = {
      consume(messageConsumer.get)
        .handle((results: Seq[AsyncQueueConsumerResult], ex: Throwable) => {
          if (ex == null) {
            results.foreach {
              case AsyncQueueConsumerResults.Ok =>
                metrics.fetchThreadConsumeMessageCounter.inc()
              case AsyncQueueConsumerResults.ConsumerException =>
                logError(s"sqsAsyncFetchMessagesThread consumer throws exception.")
                metrics.fetchThreadConsumeMessageFailedCounter.inc()
              case result => logDebug(s"sqsAsyncFetchMessagesThread consume result: ${result}")
            }
            results
          }
          else {
            logError(s"sqsAsyncFetchMessagesThread consumer uncaught exception.", ex)
            metrics.fetchThreadUncaughtExceptionCounter.inc()
            Seq.empty[AsyncQueueConsumerResult]
          }
        })
    }

    val sqsAsyncFetchMessagesThread = new Runnable {
      override def run(): Unit = {
        try {
          // Fetch and process messages from Amazon SQS in batch.
          // This thread will stop either all receiveMessages are empty or reach the maxFilesPerTrigger.
          // If there are a lot of (non-create) messages to discard, this thread will stop when SQS queue is empty.

          // TODO: This thread can keep running if receiveMessage/parseMessage/message consumer always fail.
          //  May need to stop the thread if these happen

          breakable {
            var totalMsgCnt = 0
            while (!Thread.currentThread.isInterrupted) {
              val futures: ArrayBuffer[CompletableFuture[Seq[AsyncQueueConsumerResult]]] = ArrayBuffer.empty
              var emptyMsgCnt = 0
              var errorMsgCnt = 0

              (0 until poolSize) .foreach { _ =>
                futures.append {
                  fetchAndConsumeMessage
                }
              }

              // Wait for the batch to complete
              CompletableFuture.allOf(futures: _*)
                .exceptionally((e) => {
                  logError(s"exception in sqsAsyncFetchMessagesThread allOf", e)
                  null
                })
                .whenComplete { (_, _) => {
                  futures
                    .flatMap(f => f.join)
                    .collect {
                      case AsyncQueueConsumerResults.Ok => totalMsgCnt += 1
                      case AsyncQueueConsumerResults.ReceiveEmpty => emptyMsgCnt += 1
                      case _ => errorMsgCnt += 1
                    }
                  }
                }
                .join

              logDebug(s"sqsAsyncFetchMessagesThread totalMsgCnt ${totalMsgCnt}," +
                s" last batch emptyMsgCnt ${emptyMsgCnt}, errorMsgCnt ${errorMsgCnt}.")

              if(
                (emptyMsgCnt == poolSize  && ((System.currentTimeMillis() - startTime) > waitTimeoutSecond * 1000))
                  || totalMsgCnt >= sourceOptions.maxFilesPerTrigger.getOrElse(Int.MaxValue)
              ) {
                logInfo(s"sqsAsyncFetchMessagesThread stop fetching new messages.totalMsgCnt ${totalMsgCnt}," +
                  s" last batch emptyMsgCnt ${emptyMsgCnt}, errorMsgCnt ${errorMsgCnt}.")
                break
              }
            }
          }
        } catch {
          case NonFatal(e) =>
            logWarning("sqsAsyncFetchMessagesThread uncaught exception: ", e)
            metrics.fetchThreadUncaughtExceptionCounter.inc()
        }
      }
    }

    reportTimeTaken("asyncFetch") {
      val future = asyncSqsFetchExecutor.submit(sqsAsyncFetchMessagesThread)
      awaitFetchReady(future, waitTimeoutSecond)
      future
    }
  }

  override def awaitFetchReady(future: Future[_], timeoutSecond: Long): Unit = {
      Try(future.get(timeoutSecond, TimeUnit.SECONDS)) match {
        case Failure(e) =>
          e match {
            case _: TimeoutException => logInfo(s"SQS fetch not completed in ${timeoutSecond} seconds." +
              s" Async fetch continuing at background.")
            case _ => logWarning("awaitFetchReady exception: ", e)
          }
        case _ =>
      }
  }

  private def getMessage = {
    val messageRequest = ReceiveMessageRequest.builder
      .queueUrl(queueUrl)
      .maxNumberOfMessages(MAX_SQS_BATCH_SIZE)
      .waitTimeSeconds(sourceOptions.sqsLongPollWaitTimeSeconds).build
    client.receiveMessage(messageRequest)
      .whenComplete((response, e) =>
        if (e != null ) {
          metrics.receiveMessageFailedCounter.inc()
          logError("Failed to receive message: ", e)
        }
        else metrics.receiveMessageCounter.inc(response.messages().size)
      )
  }

  override def deleteInvalidMessageIfNecessary(messageId: T): CompletableFuture[Boolean] = {
    if (sourceOptions.sqsKeepMessageForConsumerError) {
      setMessageVisibility(messageId, sourceOptions.sqsVisibilityTimeoutSeconds)
    } else {
      deleteMessage(messageId)
    }
  }

  override def handleProcessedMessage(messageId: T): CompletableFuture[Boolean] = {
    deleteMessage(messageId)
  }

  override def handleProcessedMessageBatch(messageIds: Seq[T]): Seq[CompletableFuture[Boolean]] = {
    reportTimeTaken(s"handleProcessedMessageBatch for ${messageIds.size} messages") {
      deleteMessageBatch(messageIds)
    }
  }

  override def deleteMessage(messageId: T): CompletableFuture[Boolean] = {
    val ret = new CompletableFuture[Boolean]()
    val id = messageId.asInstanceOf[String]
    deleteMessageInternal(id).whenComplete(
      deleteMessagePostProcessing(id, ret)
    )
    ret
  }

  def deleteMessageInternal(messageReceipt: String): CompletableFuture[DeleteMessageResponse] = {
      logDebug(s"deleteMessageInternal: ${messageReceipt}")
      val deleteMessageReq = DeleteMessageRequest.builder
        .queueUrl(queueUrl)
        .receiptHandle(messageReceipt)
        .build
      client.deleteMessage(deleteMessageReq)
  }

  private def deleteMessagePostProcessing(
     messageReceipt: String,
     ret: CompletableFuture[Boolean]): BiConsumer[DeleteMessageResponse, Throwable] = {
    (_, deleteException) => {
      if (deleteException != null) {
        logError(s"deleteMessagePostProcessing Exception while deleting message ${messageReceipt}",
          deleteException)
        metrics.deleteMessageFailedCounter.inc()
        ret.complete(false)
      }
      else {
        logDebug(s"Deleted message ${messageReceipt}")
        metrics.deleteMessageCounter.inc()
        ret.complete(true)
      }
    }
  }

  override def deleteMessageBatch(messageIds: Seq[T]): Seq[CompletableFuture[Boolean]] = {
    val receiptBatches = messageIds.sliding(MAX_SQS_BATCH_SIZE, MAX_SQS_BATCH_SIZE).toList

    receiptBatches.map {batch =>
      val ret = new CompletableFuture[Boolean]()
      deleteMessageBatchInternal(batch).whenComplete(
        deleteMessageBatchPostProcessing(batch, ret)
      )
      ret
    }

  }

  def deleteMessageBatchInternal(messageReceipts: Seq[T]): CompletableFuture[DeleteMessageBatchResponse] = {
    logDebug(s"deleteMessageBatchInternal: ${messageReceipts}")

    val deleteMessageBatchReq = DeleteMessageBatchRequest.builder
      .queueUrl(queueUrl)
      .entries(
        messageReceipts.zipWithIndex.map(receiptWithIndex =>
        DeleteMessageBatchRequestEntry.builder()
          .id(receiptWithIndex._2.toString)
          .receiptHandle(receiptWithIndex._1.asInstanceOf[String])
          .build).asJava)
      .build

    client.deleteMessageBatch(deleteMessageBatchReq)
  }

  private def deleteMessageBatchPostProcessing(
     messageReceipts: Seq[T],
     ret: CompletableFuture[Boolean]): BiConsumer[DeleteMessageBatchResponse, Throwable] = {
    (batchResponse, deleteException) => {
      if (deleteException != null) {
        logError(s"Exception while batch deleting messages: ", deleteException)
        // Switch to delete the messages one by one
        retryDeleteMessage(messageReceipts, ret)
      }
      else {

        val successResponse = batchResponse.successful().asScala.toSeq
        val failedResponse = batchResponse.failed().asScala.toSeq

        metrics.deleteMessageCounter.inc(successResponse.size)
        logDebug(s"deleteMessageBatchPostProcessing success: ${successResponse.size} fail: ${failedResponse.size}")
        // Switch to delete the failed messages one by one
        retryDeleteMessage(failedResponse.map{ response =>
          messageReceipts(response.id().toInt)
        }, ret)
      }
    }
  }

  private def retryDeleteMessage(messageReceipts: Seq[T], ret: CompletableFuture[Boolean]): Unit = {

    val futureList = messageReceipts.map{ receipt =>
      deleteMessage(receipt)
    }

    var failedDeleteCnt = 0
    CompletableFuture.allOf(futureList: _*)
      .whenComplete { (_, e) => {
        if (e == null) {
          futureList.map(f => f.join).collect {
            case false =>
              failedDeleteCnt += 1
            case true =>
          }
        }
        else {
          failedDeleteCnt = -1
        }
      }}
      .exceptionally((e) => {
        logError(s"exception in retryDeleteMessage allOf", e)
        failedDeleteCnt = -1
        null
      })
      .join

    ret.complete(failedDeleteCnt==0)
  }

  private def parseSqsMessage(message: Message): Option[FileMetadata[T]] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    try {
      val messageReceiptHandle = message.receiptHandle
      val messageBody = message.body
      logDebug(s"parse SQS message body: ${messageBody}")

      val messageJson = parse(messageBody).extract[JValue]
      val bucketName = (
        messageJson \ "Records" \ "s3" \ "bucket" \ "name").extract[Array[String]].head
      val eventName = (messageJson \ "Records" \ "eventName").extract[Array[String]].head

      if (eventName.contains("ObjectCreated")) {
        val timestamp = (messageJson \ "Records" \ "eventTime").extract[Array[String]].head
        val timestampMills = convertTimestampToMills(timestamp)
        val path = "s3://" +
          bucketName + "/" +
          (messageJson \ "Records" \ "s3" \ "object" \ "key").extract[Array[String]].head
        logDebug("Successfully parsed sqs message")
        metrics.parseMessageCounter.inc()

        Some(FileMetadata(URLDecoder.decode(path, StandardCharsets.UTF_8.name()),
          timestampMills, Some(messageReceiptHandle.asInstanceOf[T])))
      }
      else {
        logDebug(s"Discarded event ${eventName}")
        metrics.discardedMessageCounter.inc()
        None
      }
    } catch {
      case me: MappingException =>
        logWarning(s"Error in parsing SQS message ${message.receiptHandle}", me)
        metrics.parseMessageFailedCounter.inc()
        None
      case NonFatal(e) =>
        logWarning(s"Unexpected error while parsing SQS message ${message.receiptHandle}", e)
        metrics.parseMessageFailedCounter.inc()
        None
    }
  }

  private def consumeInternal(
                               consumer: BiFunction[FileMetadata[T], Executor, CompletableFuture[Void]]
             ): CompletableFuture[Seq[AsyncQueueConsumerResult]] = {

    getMessage
      .handle[Option[ReceiveMessageResponse]]{(r, e) =>
        if (e == null) Some(r)
        else None
      }
      .thenCompose{(responseOption) =>
        val resultFuture = new CompletableFuture[Seq[AsyncQueueConsumerResult]]

        responseOption
          .map { response =>
            if (response.messages.isEmpty) {
              resultFuture.complete(Seq(AsyncQueueConsumerResults.ReceiveEmpty))
              resultFuture
            }
            else {
              aggregateResults(response, consumer)
            }
          }
          .getOrElse {
            resultFuture.complete(Seq(AsyncQueueConsumerResults.ReceiveException))
            resultFuture
          }
        }

  }

  private def aggregateResults(response: ReceiveMessageResponse,
                               consumer: BiFunction[FileMetadata[T], Executor, CompletableFuture[Void]]
                              ): CompletableFuture[Seq[AsyncQueueConsumerResult]] = {
    var aggregatedFuture: CompletableFuture[Seq[AsyncQueueConsumerResult]] = null

    val futures: ArrayBuffer[CompletableFuture[AsyncQueueConsumerResult]] = ArrayBuffer.empty
    response.messages.asScala.foreach { message =>
      futures.append {
        parseSqsMessage(message) match {
          case Some(parsedMessage) =>
            logDebug(s"Retrieved message ${message.receiptHandle}")
            consumer
              .apply(parsedMessage, consumerExecutor)
              .handle[AsyncQueueConsumerResult]((_, ex: Throwable) => {
                if (ex == null) {
                  metrics.consumeMessageCounter.inc()
                  AsyncQueueConsumerResults.Ok
                } else {
                  metrics.consumeMessageFailedCounter.inc()
                  logError("Exception occurred while consuming message from queue.", ex)
                  AsyncQueueConsumerResults.ConsumerException
                }
              })
          case None =>
            deleteMessage(message.receiptHandle.asInstanceOf[T])
              .handle[AsyncQueueConsumerResult]((_, _) => {
                // The delete result is ignored: messages that fail to delete  will automatically
                // back to SQS and retry.
                AsyncQueueConsumerResults.ParseNone
              })
        }
      }
    }

    futures.foreach { future =>
      if (aggregatedFuture == null) {
        aggregatedFuture = future.thenApply((result) => Seq(result))
      }
      else {
        aggregatedFuture = aggregatedFuture.thenCombine[AsyncQueueConsumerResult,
          Seq[AsyncQueueConsumerResult]](future, (results, nextResult) => {
          val newResults = ArrayBuffer(results: _*)
          newResults += nextResult
          newResults
        })
      }
    }
    aggregatedFuture
  }

  override def setMessageVisibility(messageId: T,
                                    visibilityTimeoutSeconds: Int): CompletableFuture[Boolean] = {
    val messageVisibilityReq = ChangeMessageVisibilityRequest.builder
      .queueUrl(queueUrl)
      .receiptHandle(messageId.asInstanceOf[String])
      .visibilityTimeout(visibilityTimeoutSeconds).build

    client.changeMessageVisibility(messageVisibilityReq)
      .handle[Boolean](
        (_, visibilityException) => {
          if (visibilityException != null) {
            logError(s"Exception while setMessageVisibility message ${messageId}", visibilityException)
            metrics.setMessageVisibilityFailedCounter.inc()
            false
          }
          else {
            logDebug(s"setMessageVisibility message success: ${messageId}")
            metrics.setMessageVisibilityCounter.inc()
            true
          }
        }
      )
  }

  override def consume(
                consumer: Consumer[FileMetadata[T]]
              ): CompletableFuture[Seq[AsyncQueueConsumerResult]] = {
    consumeInternal(
      (msg, executor: Executor) => CompletableFuture.runAsync(() => consumer.accept(msg), executor)
    );
  }

  override def close(): Unit = {
    // order matters for a graceful shutdown - the client is closed at the end after all executors shutdown
    shutdownAndAwaitTermination(asyncSqsFetchExecutor)
    shutdownAndAwaitTermination(consumerExecutor)
    client.close()
  }
}

object AsyncSqsClientImpl {
  val MAX_POOL_SIZE = 200
  val MAX_SQS_BATCH_SIZE = 10 // limit in sqs async client - up to 10 per batch
}
