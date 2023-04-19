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

import java.io.Closeable
import java.util.concurrent.{CompletableFuture, Future}
import java.util.function.Consumer

import com.amazonaws.spark.sql.streaming.connector.FileMetadata
import com.amazonaws.spark.sql.streaming.connector.client.AsyncQueueConsumerResults.AsyncQueueConsumerResult

trait AsyncQueueClient[T] extends Closeable {
  def queueUrl: String

  def deleteInvalidMessageIfNecessary(messageId: T): CompletableFuture[Boolean]
  /**
   * @param messageId message id to delete
   * @return the returned future will be completed with true if the message is successfully deleted
   */
  def deleteMessage(messageId: T): CompletableFuture[Boolean]
  /**
   * the input messageIds can be divided into several sub batches.
   *
   * @param messageIds List of message ids to delete
   * @return a list of futures for sub batches. The returned future will be completed with true only
   *         when all messages are successfully deleted in the sub batch
   */
  def deleteMessageBatch(messageIds: Seq[T]): Seq[CompletableFuture[Boolean]]

  def handleProcessedMessage(messageId: T): CompletableFuture[Boolean]
  def handleProcessedMessageBatch(messageIds: Seq[T]): Seq[CompletableFuture[Boolean]]
  def setMessageVisibility(messageId: T,
                           visibilityTimeoutSeconds: Int): CompletableFuture[Boolean]
  def consume(consumer: Consumer[FileMetadata[T]]): CompletableFuture[Seq[AsyncQueueConsumerResult]]
  def metrics: AsyncClientMetrics
  def asyncFetch(waitTimeoutSecond: Long): Future[_]
  def awaitFetchReady(future: Future[_], timeoutSecond: Long): Unit
}

object AsyncQueueConsumerResults extends Enumeration {
  type AsyncQueueConsumerResult = Value
  val Ok, ReceiveEmpty, ParseNone, ReceiveException, ConsumerException = Value
}