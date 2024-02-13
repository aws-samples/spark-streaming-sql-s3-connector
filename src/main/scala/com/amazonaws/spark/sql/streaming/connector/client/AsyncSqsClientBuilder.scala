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

import java.time.Duration
import java.util.function.Consumer

import scala.language.implicitConversions

import com.amazonaws.spark.sql.streaming.connector.{ConnectorAwsCredentialsProvider, FileMetadata, S3ConnectorSourceOptions}
import com.amazonaws.spark.sql.streaming.connector.Utils.DEFAULT_CONNECTION_ACQUIRE_TIMEOUT
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient

class AsyncSqsClientBuilder[T] extends AsyncClientBuilder[T] {

  var options: S3ConnectorSourceOptions = _
  var consumer: (FileMetadata[T]) => Unit = _

  private val credentialsProvider: AwsCredentialsProvider =
    ConnectorAwsCredentialsProvider.builder.build().provider

  implicit def toConsumer[A](function: A => Unit): Consumer[A] = new Consumer[A]() {
    override def accept(arg: A): Unit = function.apply(arg)
  }

  override def sourceOptions(options: S3ConnectorSourceOptions): AsyncClientBuilder[T] = {
    this.options = options
    this
  }

  override def consumer(function: FileMetadata[T] => Unit): AsyncClientBuilder[T] = {
    this.consumer = function
    this
  }

  def build(): AsyncQueueClient[T] = {
    require(options!=null, "sourceOptions can't be null")
    require(consumer!=null, "sqs message consumer can't be null")

    val asyncSqsClient = getAsyncSQSClient(options)
    new AsyncSqsClientImpl(asyncSqsClient, options, Some(consumer))
  }

  private def getAsyncSQSClient(options: S3ConnectorSourceOptions): SqsAsyncClient = {
    val retryPolicy = RetryPolicy.builder
      .numRetries(options.sqsMaxRetries)
      .retryCondition(RetryCondition.defaultRetryCondition)
      .backoffStrategy(BackoffStrategy.defaultThrottlingStrategy)
      .build

    val clientOverrideConfiguration = ClientOverrideConfiguration.builder
      .retryPolicy(retryPolicy)
      .build

    SqsAsyncClient.builder
      .httpClient(
        NettyNioAsyncHttpClient
        .builder()
        .maxConcurrency(options.sqsMaxConcurrency)
        .connectionAcquisitionTimeout(Duration.ofSeconds(DEFAULT_CONNECTION_ACQUIRE_TIMEOUT))
        .build()
      )
      .region(Region.of(options.queueRegion))
      .overrideConfiguration(clientOverrideConfiguration)
      .credentialsProvider(credentialsProvider)
      .build

  }
}
