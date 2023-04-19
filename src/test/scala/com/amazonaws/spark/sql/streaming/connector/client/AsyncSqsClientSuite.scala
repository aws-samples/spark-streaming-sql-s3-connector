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

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.spark.sql.streaming.connector.{FileMetadata, S3ConnectorSourceOptions, S3ConnectorTestBase}
import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions.{MAX_FILES_PER_TRIGGER, QUEUE_URL, SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR, SQS_MAX_CONCURRENCY}
import com.amazonaws.spark.sql.streaming.connector.TestUtils.{doReturnMock, verifyMetrics}
import com.amazonaws.spark.sql.streaming.connector.client.AsyncSqsClientImpl.MAX_SQS_BATCH_SIZE
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

class AsyncSqsClientSuite extends S3ConnectorTestBase{

  var asyncClient: AsyncSqsClientImpl[String] = _
  var mockAsyncClient: SqsAsyncClient = _

  val SUCCESS_RECEIVE_MSG_CNT = 21
  val EXCEPTION_RECEIVE_MSG_CNT = 16
  val SUCCESS_CONSUME_CNT = 15
  val SUCCESS_BATCH_DELETE_MSG_CNT = 21
  val FAIL_BATCH_DELETE_MSG_CNT = 16

  val objectCreatedMsg: Message = Message.builder()
    .messageId("messageId1")
    .receiptHandle("receiptHandle1")
    .body(
      """{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-2",
        |"eventTime":"2023-02-14T22:53:31.748Z",
        |"eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:userIdentity1"},
        |"requestParameters":{"sourceIPAddress":"1.1.1.1"},"responseElements":{"x-amz-request-id":"requestid",
        |"x-amz-id-2":"test-x-amz-id-2"},
        |"s3":{"s3SchemaVersion":"1.0","configurationId":"confId1","bucket":{"name":"testbucket",
        |"ownerIdentity":{"principalId":"ownerIdentity`"},"arn":"arn:aws:s3:::testbucket"},
        |"object":{"key":"valPartition%3Dpart1678486410_0/part-00000-created.csv",
        |"size":16,"eTag":"etag","sequencer":"1"}}}]}""".stripMargin)
    .build

  val objectRemovedMsg: Message = Message.builder()
    .messageId("messageId1")
    .receiptHandle("receiptHandle1")
    .body(
      """{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-2",
        |"eventTime":"2023-02-14T22:53:31.748Z",
        |"eventName":"ObjectRemoved:Delete","userIdentity":{"principalId":"AWS:userIdentity1"},
        |"requestParameters":{"sourceIPAddress":"1.1.1.1"},"responseElements":{"x-amz-request-id":"requestid",
        |"x-amz-id-2":"test-x-amz-id-2"},
        |"s3":{"s3SchemaVersion":"1.0","configurationId":"confId1","bucket":{"name":"testbucket",
        |"ownerIdentity":{"principalId":"ownerIdentity`"},"arn":"arn:aws:s3:::testbucket"},
        |"object":{"key":"part-00001-deleted.csv",
        |"size":16,"eTag":"etag","sequencer":"1"}}}]}""".stripMargin)
    .build

  val objectCreatedBadMsg: Message = Message.builder()
    .messageId("messageId1")
    .receiptHandle("receiptHandle1")
    .body(
      """{"Records":"bad message"}""".stripMargin)
    .build

  before {
    mockAsyncClient = mock(classOf[SqsAsyncClient])
    asyncClient = new AsyncSqsClientImpl(
      mockAsyncClient,
      S3ConnectorSourceOptions(defaultOptionMap),
      None
    )
  }

  after {
    asyncClient.close()
  }

  def setMockReceiveMassageResponse(mockClient: SqsAsyncClient, responseMessage: Message,
                                    batchSize: Int = 1) : Unit = {
    val receiveMessageResponse = new CompletableFuture[ReceiveMessageResponse]
    doReturnMock(receiveMessageResponse).when(mockClient).receiveMessage(any().asInstanceOf[ReceiveMessageRequest])
    receiveMessageResponse.complete(
      ReceiveMessageResponse.builder()
        .messages(
          (0 until batchSize).map { _ => responseMessage } : _*
        )
        .build
    )
  }

  def setMockReceiveMassageEmptyResponse(mockClient: SqsAsyncClient): Unit = {
    val receiveMessageResponse = new CompletableFuture[ReceiveMessageResponse]
    doReturnMock(receiveMessageResponse).when(mockClient).receiveMessage(any().asInstanceOf[ReceiveMessageRequest])
    receiveMessageResponse.complete(
      ReceiveMessageResponse.builder()
        .build
    )
  }

  def setMockReceiveMassageResponseFirstSuccessAndRestEmpty(mockClient: SqsAsyncClient,
                                                            responseMessage: Message,
                                                            firstCnt: Int
                                               ): Unit = {

    when(mockClient.receiveMessage(any().asInstanceOf[ReceiveMessageRequest]))
      .thenAnswer(new Answer[Any]() {
      private var count = 0

      override def answer(invocation: InvocationOnMock): Any = {
        val receiveMessageResponse = new CompletableFuture[ReceiveMessageResponse]
        if(count < firstCnt) {
          receiveMessageResponse.complete(
            ReceiveMessageResponse.builder()
              .messages(responseMessage)
              .build
          )
        } else {
          receiveMessageResponse.complete(
            ReceiveMessageResponse.builder()
              .build
          )
        }
        count = count + 1
        receiveMessageResponse
      }
    })
  }

  def setMockReceiveMassageResponseFirstSuccessThenExceptionAndRestEmpty(mockClient: SqsAsyncClient,
                                                                         responseMessage: Message,
                                                                         firstCnt: Int, exceptionCnt: Int
                                                    ): Unit = {

    when(mockClient.receiveMessage(any().asInstanceOf[ReceiveMessageRequest]))
      .thenAnswer(new Answer[Any]() {
        private var count = 0

        override def answer(invocation: InvocationOnMock): Any = {
          val receiveMessageResponse = new CompletableFuture[ReceiveMessageResponse]
          if(count < firstCnt) {
            receiveMessageResponse.complete(
              ReceiveMessageResponse.builder()
                .messages(responseMessage)
                .build
            )
          } else if (count < firstCnt + exceptionCnt) {
            receiveMessageResponse.completeExceptionally(
              new RuntimeException("exception in receive message")
            )
          } else {
            receiveMessageResponse.complete(
              ReceiveMessageResponse.builder()
                .build
            )
          }
          count = count + 1
          receiveMessageResponse
        }
      })
  }

  def setMockReceiveMassageException(mockClient: SqsAsyncClient): Unit = {
    val receiveMessageResponse = new CompletableFuture[ReceiveMessageResponse]
    doReturnMock(receiveMessageResponse).when(mockClient).receiveMessage(any().asInstanceOf[ReceiveMessageRequest])
    receiveMessageResponse.completeExceptionally(
      new RuntimeException("exception in receive message")
    )
  }

  def setMockDeleteMassageResponse(mockClient: SqsAsyncClient): Unit = {
    val deleteMessageResponse = new CompletableFuture[DeleteMessageResponse]
    doReturnMock(deleteMessageResponse).when(mockClient).deleteMessage(any().asInstanceOf[DeleteMessageRequest])
    deleteMessageResponse.complete(
      DeleteMessageResponse.builder().build
    )
  }

  def setMockDeleteMassageBatchResponse(mockClient: SqsAsyncClient,
                                        successCnt: Int,
                                        failCnt: Int
                                       ): Unit = {

    when(mockClient.deleteMessageBatch(any().asInstanceOf[DeleteMessageBatchRequest]))
      .thenAnswer(new Answer[Any]() {
        private var count = 1

        override def answer(invocation: InvocationOnMock): Any = {
          val deleteMessageBatchResponse = new CompletableFuture[DeleteMessageBatchResponse]
          val currentMaxCnt = count * MAX_SQS_BATCH_SIZE

          val (successMsgs, failedMsgs) = if (currentMaxCnt <= successCnt) {
            (Some((currentMaxCnt-MAX_SQS_BATCH_SIZE until currentMaxCnt).map { i => s"delete${i}"}),
              None)
          } else if (currentMaxCnt < successCnt + MAX_SQS_BATCH_SIZE) {
            val successDelete = Some((currentMaxCnt-MAX_SQS_BATCH_SIZE until successCnt).map { i => s"delete${i}"})
            val slotLeft = MAX_SQS_BATCH_SIZE - successDelete.getOrElse(Seq.empty[String]).size
            val failDelete = Some(
              (successCnt to currentMaxCnt-MAX_SQS_BATCH_SIZE + Math.min(slotLeft, failCnt)).map { i =>
                s"delete${i}"
              })

            (successDelete, failDelete)
          } else {
            ( None,
              Some(
              (currentMaxCnt-MAX_SQS_BATCH_SIZE until Math.min(currentMaxCnt, successCnt + failCnt))
                .map { i => s"delete${i}"}
            ))
          }

          count += 1
          deleteMessageBatchResponse.complete(
            DeleteMessageBatchResponse.builder()
              .successful(buildMessageBatchResultEntries(successMsgs): _*)
              .failed(buildBatchResultErrorEntries(failedMsgs, successMsgs.size): _*)
              .build
          )
          deleteMessageBatchResponse
        }
      })
  }

  def buildMessageBatchResultEntries(messagesOption: Option[Seq[String]]): Seq[DeleteMessageBatchResultEntry] = {
    messagesOption.map {messages =>
      messages.zipWithIndex.map { messageWithIndex =>
        DeleteMessageBatchResultEntry.builder()
          .id(messageWithIndex._2.toString)
          .build
      }
    }.getOrElse(Seq.empty[DeleteMessageBatchResultEntry])
  }

  def buildBatchResultErrorEntries(messagesOption: Option[Seq[String]], startIndex: Int): Seq[BatchResultErrorEntry] = {
    messagesOption.map {messages =>
      messages.zipWithIndex.map { messageWithIndex =>
        BatchResultErrorEntry.builder()
          .id((startIndex + messageWithIndex._2).toString )
          .build
      }
    }.getOrElse(Seq.empty[BatchResultErrorEntry])
  }

  def setMockChangeMessageVisibilityResponse(mockClient: SqsAsyncClient): Unit = {
    val changeMessageVisibilityResponse = new CompletableFuture[ChangeMessageVisibilityResponse]
    doReturnMock(changeMessageVisibilityResponse).when(mockClient).changeMessageVisibility(
      any().asInstanceOf[ChangeMessageVisibilityRequest])
    changeMessageVisibilityResponse.complete(
      ChangeMessageVisibilityResponse.builder().build
    )
  }

  def setMockDeleteMassageBatchException(mockClient: SqsAsyncClient): Unit = {
    val deleteMessageBatchResponse = new CompletableFuture[DeleteMessageBatchResponse]
    doReturnMock(deleteMessageBatchResponse).when(mockClient).deleteMessageBatch(
      any().asInstanceOf[DeleteMessageBatchRequest])
    deleteMessageBatchResponse.completeExceptionally(
      SdkException.builder()
        .message("exception in delete message")
        .build()
    )
  }

  def setMockChangeMessageVisibilityException(mockClient: SqsAsyncClient): Unit = {
    val changeMessageVisibilityResponse = new CompletableFuture[ChangeMessageVisibilityResponse]
    doReturnMock(changeMessageVisibilityResponse)
      .when(mockClient)
      .changeMessageVisibility(any().asInstanceOf[ChangeMessageVisibilityRequest])
    changeMessageVisibilityResponse.completeExceptionally(
      SdkException.builder()
        .message("exception in change message visibility")
        .build()
    )
  }

  def setMockDeleteMassageException(mockClient: SqsAsyncClient): Unit = {
    val deleteMessageResponse = new CompletableFuture[DeleteMessageResponse]
    doReturnMock(deleteMessageResponse).when(mockClient).deleteMessage(any().asInstanceOf[DeleteMessageRequest])
    deleteMessageResponse.completeExceptionally(
      SdkException.builder()
        .message("exception in delete message")
        .build()
    )
  }


  test("AsyncSqsClientImpl parse ObjectCreated message") {

    val pathList = new ArrayBuffer[String]()

    val createBatchSize = 5
    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg, createBatchSize)

    val ret = asyncClient.consume(
      (data: FileMetadata[String]) => {
        pathList.append(data.filePath)
      }
    ).get

    ret.size shouldBe createBatchSize
    ret.foreach(r => r shouldBe  AsyncQueueConsumerResults.Ok)
    pathList.size shouldBe createBatchSize
    pathList.foreach(p => p shouldEqual "s3://testbucket/valPartition=part1678486410_0/part-00000-created.csv")
    verify(mockAsyncClient, times(1)).receiveMessage(
      any().asInstanceOf[ReceiveMessageRequest]
    )
    verify(mockAsyncClient, times(0)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

    verifyMetrics(asyncClient.metrics,
      expectedReceiveMessageCount = createBatchSize,
      expectedConsumeMessageCount = createBatchSize,
      expectedParseMessageCount = createBatchSize
    )

  }

  test("AsyncSqsClientImpl parse bad ObjectCreated message") {

    val pathList = new ArrayBuffer[String]()

    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedBadMsg)
    setMockDeleteMassageResponse(mockAsyncClient)

    val ret = asyncClient.consume(
      (data: FileMetadata[String]) => {
        pathList.append(data.filePath)
      }
    ).get

    ret.size shouldBe 1
    ret.head shouldBe AsyncQueueConsumerResults.ParseNone
    pathList.size shouldBe 0
    verify(mockAsyncClient, times(1)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

    verifyMetrics(asyncClient.metrics,
      expectedReceiveMessageCount = 1,
      expectedParseMessageFailedCount = 1,
      expectedDeleteMessageCount = 1
    )

  }

  test("AsyncSqsClientImpl parse ObjectRemoved message") {

    val pathList = new ArrayBuffer[String]()
    setMockReceiveMassageResponse(mockAsyncClient, objectRemovedMsg)
    setMockDeleteMassageResponse(mockAsyncClient)

    val ret = asyncClient.consume(
      (data: FileMetadata[String]) => {
        pathList.append(data.filePath)
      }
    ).get

    ret.size shouldBe 1
    ret.head shouldBe AsyncQueueConsumerResults.ParseNone
    pathList.size shouldBe 0
    verify(mockAsyncClient, times(1)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

    verifyMetrics(asyncClient.metrics,
      expectedReceiveMessageCount = 1,
      expectedDeleteMessageCount = 1,
      expectedDiscardedMessageCount = 1
    )
  }

  test("AsyncSqsClientImpl receive message exception") {

    val pathList = new ArrayBuffer[String]()
    setMockReceiveMassageException(mockAsyncClient)

    val ret = asyncClient.consume(
      (data: FileMetadata[String]) => {
        pathList.append(data.filePath)
      }
    ).get
    ret.size shouldBe 1
    ret.head shouldBe AsyncQueueConsumerResults.ReceiveException
    pathList.size shouldBe 0

    verifyMetrics(asyncClient.metrics,
      expectedReceiveMessageFailedCount = 1
    )
  }

  test("AsyncSqsClientImpl receive no message") {
    val pathList = new ArrayBuffer[String]()

    setMockReceiveMassageEmptyResponse(mockAsyncClient)

    val ret = asyncClient.consume(
      (data: FileMetadata[String]) => {
        pathList.append(data.filePath)
      }
    ).get
    ret.size shouldBe 1
    ret.head shouldBe AsyncQueueConsumerResults.ReceiveEmpty
    pathList.size shouldBe 0
    verify(mockAsyncClient, times(1)).receiveMessage(
      any().asInstanceOf[ReceiveMessageRequest]
    )
    verify(mockAsyncClient, times(0)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

    verifyMetrics(asyncClient.metrics)
  }

  test("AsyncSqsClientImpl consumer exception") {

    val pathList = new ArrayBuffer[String]()
    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg)

    val ret = asyncClient.consume(
      (data: FileMetadata[String]) => {
        throw new RuntimeException("exception in consumer")
        pathList.append(data.filePath)
      }
    ).get

    ret.size shouldBe 1
    ret.head shouldBe AsyncQueueConsumerResults.ConsumerException
    pathList.size shouldBe 0

    verifyMetrics(asyncClient.metrics,
      expectedReceiveMessageCount = 1,
      expectedParseMessageCount = 1,
      expectedConsumeMessageFailedCount = 1
    )
  }

  test("AsyncSqsClientImpl deleteErrorMessageIfNecessary") {

    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg)
    setMockDeleteMassageResponse(mockAsyncClient)

    val ret = asyncClient.deleteInvalidMessageIfNecessary("delete1")
    ret.get shouldBe true
    verify(mockAsyncClient, times(1)).deleteMessage(
      DeleteMessageRequest.builder()
        .receiptHandle("delete1")
        .queueUrl(defaultOptionMap(QUEUE_URL))
        .build
    )

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageCount = 1)
  }

  test("AsyncSqsClientImpl deleteErrorMessageIfNecessary exception") {
    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg)
    setMockDeleteMassageException(mockAsyncClient)

    val deleteResult = asyncClient.deleteInvalidMessageIfNecessary("delete1")

    deleteResult.get shouldBe false

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageFailedCount = 1)
  }

  test("AsyncSqsClientImpl deleteErrorMessageIfNecessary with SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR=true") {

    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg)
    setMockDeleteMassageResponse(mockAsyncClient)
    setMockChangeMessageVisibilityResponse(mockAsyncClient)

    val testAsyncClient = new AsyncSqsClientImpl[String](
      mockAsyncClient,
      S3ConnectorSourceOptions(defaultOptionMap + (SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR -> "true")),
      None
    )

    val ret = testAsyncClient.deleteInvalidMessageIfNecessary("delete1")
    ret.get shouldBe true
    verify(mockAsyncClient, times(0)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])
    verify(mockAsyncClient, times(1)).changeMessageVisibility(
      any().asInstanceOf[ChangeMessageVisibilityRequest])

    verifyMetrics(testAsyncClient.metrics, expectedSetMessageVisibilityCount = 1)
  }

  test("AsyncSqsClientImpl deleteErrorMessageIfNecessary exception with SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR=true") {
    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg)
    setMockDeleteMassageResponse(mockAsyncClient)
    setMockChangeMessageVisibilityException(mockAsyncClient)

    val testAsyncClient = new AsyncSqsClientImpl[String](
      mockAsyncClient,
      S3ConnectorSourceOptions(defaultOptionMap + (SQS_KEEP_MESSAGE_FOR_CONSUMER_ERROR -> "true")),
      None
    )

    val deleteResult = testAsyncClient.deleteInvalidMessageIfNecessary("delete1")

    deleteResult.get shouldBe false
    verifyMetrics(testAsyncClient.metrics, expectedSetMessageVisibilityFailedCount = 1)
  }

  test("AsyncSqsClientImpl handleProcessedMessage") {

    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg)
    setMockDeleteMassageResponse(mockAsyncClient)

    val ret = asyncClient.handleProcessedMessage("processed1")
    ret.get shouldBe true
    verify(mockAsyncClient, times(1)).deleteMessage(
      DeleteMessageRequest.builder()
        .receiptHandle("processed1")
        .queueUrl(defaultOptionMap(QUEUE_URL))
        .build
    )
  }

  test("AsyncSqsClientImpl handleProcessedMessage exception") {
    setMockReceiveMassageResponse(mockAsyncClient, objectCreatedMsg)
    setMockDeleteMassageException(mockAsyncClient)

    val processResult = asyncClient.handleProcessedMessage("processed1")

    processResult.get shouldBe false

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageFailedCount = 1)
  }

  test ("run SQS async fetch") {
    setMockReceiveMassageResponseFirstSuccessAndRestEmpty(mockAsyncClient, objectCreatedMsg, SUCCESS_RECEIVE_MSG_CNT)
    setMockDeleteMassageResponse(mockAsyncClient)

    val sourceOptions = S3ConnectorSourceOptions(defaultOptionMap)
    var fetchThreadAsyncClient: AsyncQueueClient[String] = null

    fetchThreadAsyncClient = new AsyncSqsClientImpl(
      mockAsyncClient,
      sourceOptions,
      Some((data: FileMetadata[String]) => {
        logInfo(s"consumer data: ${data}")
        data.messageId.map(fetchThreadAsyncClient.handleProcessedMessage)
      })
    )

    fetchThreadAsyncClient.asyncFetch(sourceOptions.queueFetchWaitTimeoutSeconds)

    fetchThreadAsyncClient.close()
    logInfo(s"metrics: ${fetchThreadAsyncClient.metrics.json}")
    verifyMetrics(fetchThreadAsyncClient.metrics,
      expectedReceiveMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedConsumeMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedParseMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedFetchThreadConsumeMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedDeleteMessageCount = SUCCESS_RECEIVE_MSG_CNT
    )
  }

  test ("run SQS async fetch limit by MAX_FILES_PER_TRIGGER when SQS_MAX_CONCURRENCY=1") {
    setMockReceiveMassageResponseFirstSuccessAndRestEmpty(mockAsyncClient, objectCreatedMsg, SUCCESS_RECEIVE_MSG_CNT)
    setMockDeleteMassageResponse(mockAsyncClient)

    var fetchThreadAsyncClient: AsyncQueueClient[String] = null
    val test_max_files_per_trigger = SUCCESS_RECEIVE_MSG_CNT/2
    val sourceOptions = S3ConnectorSourceOptions(defaultOptionMap
      + (MAX_FILES_PER_TRIGGER -> s"${test_max_files_per_trigger}",
         SQS_MAX_CONCURRENCY -> "1" // as asyncFetch run in batches, it is possible to fetch more files than
                                    // MAX_FILES_PER_TRIGGER if SQS_MAX_CONCURRENCY > 1
      ))

    fetchThreadAsyncClient = new AsyncSqsClientImpl[String](
      mockAsyncClient,
      sourceOptions,
      Some((data: FileMetadata[String]) => {
        logInfo(s"consumer data: ${data}")
        data.messageId.map(fetchThreadAsyncClient.handleProcessedMessage)
      })
    )

    fetchThreadAsyncClient.asyncFetch(sourceOptions.queueFetchWaitTimeoutSeconds)

    fetchThreadAsyncClient.close()
    logInfo(s"metrics: ${fetchThreadAsyncClient.metrics.json}")
    verifyMetrics(fetchThreadAsyncClient.metrics,
      expectedReceiveMessageCount = test_max_files_per_trigger,
      expectedConsumeMessageCount = test_max_files_per_trigger,
      expectedParseMessageCount = test_max_files_per_trigger,
      expectedFetchThreadConsumeMessageCount = test_max_files_per_trigger,
      expectedDeleteMessageCount = test_max_files_per_trigger
    )
  }

  test ("run SQS async fetch receive message exception") {

    setMockReceiveMassageResponseFirstSuccessThenExceptionAndRestEmpty(mockAsyncClient, objectCreatedMsg,
      SUCCESS_RECEIVE_MSG_CNT, EXCEPTION_RECEIVE_MSG_CNT)

    val sourceOptions = S3ConnectorSourceOptions(defaultOptionMap)
    val fetchThreadAsyncClient = new AsyncSqsClientImpl[String](
      mockAsyncClient,
      sourceOptions,
      Some((data: FileMetadata[String]) => {
        logInfo(s"consumer data: ${data}")
      })
    )

    fetchThreadAsyncClient.asyncFetch(sourceOptions.queueFetchWaitTimeoutSeconds)

    fetchThreadAsyncClient.close()
    logInfo(s"metrics: ${fetchThreadAsyncClient.metrics.json}")
    verifyMetrics(fetchThreadAsyncClient.metrics,
      expectedReceiveMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedConsumeMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedParseMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedFetchThreadConsumeMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedReceiveMessageFailedCount = EXCEPTION_RECEIVE_MSG_CNT
    )
  }

  test ("run SQS async fetch consumer exception") {
    setMockReceiveMassageResponseFirstSuccessAndRestEmpty(mockAsyncClient, objectCreatedMsg, SUCCESS_RECEIVE_MSG_CNT)

    val count: LongAdder = new LongAdder()
    val sourceOptions = S3ConnectorSourceOptions(defaultOptionMap)

    val fetchThreadAsyncClient = new AsyncSqsClientImpl[String](
      mockAsyncClient,
      sourceOptions,
      Some((data: FileMetadata[String]) => {
        logInfo(s"consumer data: ${data}")
        count.increment()
        if(count.sum() > SUCCESS_CONSUME_CNT) {
          throw new RuntimeException("runtime exception in consumer")
        }
      })
    )

    fetchThreadAsyncClient.asyncFetch(sourceOptions.queueFetchWaitTimeoutSeconds)

    fetchThreadAsyncClient.close()
    logInfo(s"metrics: ${fetchThreadAsyncClient.metrics.json}")
    verifyMetrics(fetchThreadAsyncClient.metrics,
      expectedReceiveMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedConsumeMessageCount = SUCCESS_CONSUME_CNT,
      expectedParseMessageCount = SUCCESS_RECEIVE_MSG_CNT,
      expectedConsumeMessageFailedCount = SUCCESS_RECEIVE_MSG_CNT - SUCCESS_CONSUME_CNT,
      expectedFetchThreadConsumeMessageFailedCount = SUCCESS_RECEIVE_MSG_CNT - SUCCESS_CONSUME_CNT,
      expectedFetchThreadConsumeMessageCount = SUCCESS_CONSUME_CNT
    )
  }

  test(s"AsyncSqsClientImpl deleteMessageBatch with less than ${MAX_SQS_BATCH_SIZE} messages") {
    val messages = Seq("delete0", "delete1")

    setMockDeleteMassageBatchResponse(mockAsyncClient, messages.size, 0)

    val ret = asyncClient.deleteMessageBatch(messages)
    ret.size shouldBe 1
    ret.head.get shouldBe true

    verify(mockAsyncClient, times(1)).deleteMessageBatch(
      DeleteMessageBatchRequest.builder()
        .entries(
          messages.zipWithIndex.map(receiptWithIndex =>
            DeleteMessageBatchRequestEntry.builder()
              .id(receiptWithIndex._2.toString)
              .receiptHandle(receiptWithIndex._1)
              .build).asJava
        )
        .queueUrl(defaultOptionMap(QUEUE_URL))
        .build
    )

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageCount = 2)
  }

  test(s"AsyncSqsClientImpl deleteMessageBatch with more than ${MAX_SQS_BATCH_SIZE} messages") {

    val messages = (0 until SUCCESS_BATCH_DELETE_MSG_CNT).map(i => s"success_delete${i}")

    setMockDeleteMassageBatchResponse(mockAsyncClient, SUCCESS_BATCH_DELETE_MSG_CNT, 0)

    val subBatchCnt = SUCCESS_BATCH_DELETE_MSG_CNT/MAX_SQS_BATCH_SIZE + 1
    val ret = asyncClient.deleteMessageBatch(messages)

    ret.size shouldBe subBatchCnt
    ret.foreach(f => f.get shouldBe true)

    verify(mockAsyncClient, times(subBatchCnt))
      .deleteMessageBatch(any().asInstanceOf[DeleteMessageBatchRequest])

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageCount = SUCCESS_BATCH_DELETE_MSG_CNT)
  }

  test("AsyncSqsClientImpl deleteMessageBatch with part of batch fail but individual delete success") {

    val totalCnt = SUCCESS_BATCH_DELETE_MSG_CNT + FAIL_BATCH_DELETE_MSG_CNT
    val messages = (0 until totalCnt).map(i => s"delete${i}")

    setMockDeleteMassageBatchResponse(mockAsyncClient, SUCCESS_BATCH_DELETE_MSG_CNT, FAIL_BATCH_DELETE_MSG_CNT)
    setMockDeleteMassageResponse(mockAsyncClient)

    val subBatchCnt = totalCnt/MAX_SQS_BATCH_SIZE + 1
    val ret = asyncClient.deleteMessageBatch(messages)

    ret.size shouldBe subBatchCnt
    ret.foreach(f => f.get shouldBe true)

    verify(mockAsyncClient, times(subBatchCnt))
      .deleteMessageBatch(any().asInstanceOf[DeleteMessageBatchRequest])
    verify(mockAsyncClient, times(FAIL_BATCH_DELETE_MSG_CNT)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageCount = totalCnt)
  }

  val batchTestData =
    Table(
      ("successCnt", "failCnt"),  // First tuple defines column names
      (0, 0),
      (SUCCESS_BATCH_DELETE_MSG_CNT, 0),
      (0, FAIL_BATCH_DELETE_MSG_CNT),
      (SUCCESS_BATCH_DELETE_MSG_CNT, FAIL_BATCH_DELETE_MSG_CNT),
      (20, 0),
      (0, 30),
      (20, 30)
    )

  forAll (batchTestData) { (successCnt: Int, failCnt: Int) =>
    test("AsyncSqsClientImpl deleteMessageBatch with combinations of " +
      s"batch ${successCnt} msgs success, batch ${failCnt} msgs fail and all individual delete fail") {
        logInfo(s"successCnt: ${successCnt}, failCnt: ${failCnt}")
        val totalCnt = successCnt + failCnt
        val messages = (0 until totalCnt).map(i => s"delete${i}")

        setMockDeleteMassageBatchResponse(mockAsyncClient, successCnt, failCnt)
        setMockDeleteMassageException(mockAsyncClient)

        val subBatchCnt = totalCnt/MAX_SQS_BATCH_SIZE + (if (totalCnt%MAX_SQS_BATCH_SIZE == 0) 0 else 1)
        val ret = asyncClient.deleteMessageBatch(messages)

        ret.size shouldBe subBatchCnt
        (0 until successCnt/MAX_SQS_BATCH_SIZE).foreach(i => ret(i).get shouldBe true)

        if (failCnt == 0) {
          (successCnt/MAX_SQS_BATCH_SIZE until subBatchCnt).foreach(i => ret(i).get shouldBe true)
        } else {
          (successCnt/MAX_SQS_BATCH_SIZE until subBatchCnt).foreach(i => ret(i).get shouldBe false)
        }

        verify(mockAsyncClient, times(subBatchCnt))
          .deleteMessageBatch(any().asInstanceOf[DeleteMessageBatchRequest])
        verify(mockAsyncClient, times(failCnt)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

        verifyMetrics(asyncClient.metrics,
          expectedDeleteMessageCount = successCnt,
          expectedDeleteMessageFailedCount = failCnt
        )
      }
  }

  test("AsyncSqsClientImpl deleteMessageBatch throw exception but individual delete success") {
    val messages = (0 until SUCCESS_BATCH_DELETE_MSG_CNT).map(i => s"delete${i}")

    setMockDeleteMassageBatchException(mockAsyncClient)
    setMockDeleteMassageResponse(mockAsyncClient)

    val subBatchCnt = SUCCESS_BATCH_DELETE_MSG_CNT/MAX_SQS_BATCH_SIZE + 1
    val ret = asyncClient.deleteMessageBatch(messages)

    ret.size shouldBe subBatchCnt
    ret.foreach(f => f.get shouldBe true)

    verify(mockAsyncClient, times(subBatchCnt))
      .deleteMessageBatch(any().asInstanceOf[DeleteMessageBatchRequest])
    verify(mockAsyncClient, times(SUCCESS_BATCH_DELETE_MSG_CNT)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

    verifyMetrics(asyncClient.metrics,
      expectedDeleteMessageCount = SUCCESS_BATCH_DELETE_MSG_CNT
    )
  }

  test("AsyncSqsClientImpl deleteMessageBatch throw exception but individual delete fail") {
    val messages = (0 until FAIL_BATCH_DELETE_MSG_CNT).map(i => s"delete${i}")

    setMockDeleteMassageBatchException(mockAsyncClient)
    setMockDeleteMassageException(mockAsyncClient)

    val subBatchCnt = FAIL_BATCH_DELETE_MSG_CNT/MAX_SQS_BATCH_SIZE + 1
    val ret = asyncClient.deleteMessageBatch(messages)

    ret.size shouldBe subBatchCnt
    ret.foreach(f => f.get shouldBe false)

    verify(mockAsyncClient, times(subBatchCnt))
      .deleteMessageBatch(any().asInstanceOf[DeleteMessageBatchRequest])
    verify(mockAsyncClient, times(FAIL_BATCH_DELETE_MSG_CNT)).deleteMessage(any().asInstanceOf[DeleteMessageRequest])

    verifyMetrics(asyncClient.metrics,
      expectedDeleteMessageFailedCount = FAIL_BATCH_DELETE_MSG_CNT
    )
  }

  test("AsyncSqsClientImpl handleProcessedMessageBatch") {

    setMockDeleteMassageBatchResponse(mockAsyncClient, 1, 0)
    setMockDeleteMassageException(mockAsyncClient)

    val processResult = asyncClient.handleProcessedMessageBatch(Seq("processed1"))
    processResult.size shouldBe 1
    processResult.head.get shouldBe true

    verify(mockAsyncClient, times(1)).deleteMessageBatch(
      DeleteMessageBatchRequest.builder()
        .entries(
            DeleteMessageBatchRequestEntry.builder()
              .id("0")
              .receiptHandle("processed1")
              .build
        )
        .queueUrl(defaultOptionMap(QUEUE_URL))
        .build
    )

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageCount = 1)
  }

  test("AsyncSqsClientImpl handleProcessedMessageBatch has a list of None as input") {

    setMockDeleteMassageBatchResponse(mockAsyncClient, 1, 0)
    setMockDeleteMassageException(mockAsyncClient)

    val processResult = asyncClient.handleProcessedMessageBatch(Seq(None, None, None).flatten)
    processResult.size shouldBe 0

    verify(mockAsyncClient, times(0)).deleteMessageBatch(
      any().asInstanceOf[DeleteMessageBatchRequest]
    )
    verifyMetrics(asyncClient.metrics)
  }

  test("AsyncSqsClientImpl handleProcessedMessageBatch exception") {
    setMockDeleteMassageBatchException(mockAsyncClient)
    setMockDeleteMassageException(mockAsyncClient)

    val processResult = asyncClient.handleProcessedMessageBatch(Seq("processed1"))
    processResult.size shouldBe 1
    processResult.head.get shouldBe false

    verify(mockAsyncClient, times(1)).deleteMessageBatch(
      DeleteMessageBatchRequest.builder()
        .entries(
          DeleteMessageBatchRequestEntry.builder()
            .id("0")
            .receiptHandle("processed1")
            .build
        )
        .queueUrl(defaultOptionMap(QUEUE_URL))
        .build
    )

    verify(mockAsyncClient, times(1)).deleteMessage(
      DeleteMessageRequest.builder()
        .receiptHandle("processed1")
        .queueUrl(defaultOptionMap(QUEUE_URL))
        .build
    )

    verifyMetrics(asyncClient.metrics, expectedDeleteMessageFailedCount = 1)
  }

}
