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

import java.io.Closeable

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import software.amazon.awssdk.auth.credentials.{AwsCredentials, AwsCredentialsProvider, DefaultCredentialsProvider}

/**
 * Serializable interface providing a method executors can call to obtain an
 * AWSCredentialsProvider instance for authenticating to AWS services.
 */
sealed trait ConnectorAwsCredentialsProvider extends Serializable with Closeable {
  def provider: AwsCredentialsProvider
  override def close(): Unit = {}
}

case class RetryableDefaultCredentialsProvider() extends AwsCredentialsProvider with Closeable {
//    private val provider = DefaultCredentialsProvider.create()
  
  private val provider = DefaultCredentialsProvider.builder()
    .asyncCredentialUpdateEnabled(true)
    .build()

  private val MAX_ATTEMPT = 10
  private val SLEEP_TIME = 1000
  
  override def resolveCredentials(): AwsCredentials = {
    @tailrec
    def getCredentialsWithRetry(retries: Int): AwsCredentials = {
      Try {
        provider.resolveCredentials()
      } match {
        case Success(credentials) =>
          credentials
        case Failure(_) if retries > 0 =>
          Thread.sleep(SLEEP_TIME)
          getCredentialsWithRetry(retries - 1) // Recursive call to retry
        case Failure(exception) =>
          throw exception
      }
    }

    getCredentialsWithRetry(MAX_ATTEMPT)
  }

  override def close(): Unit = {
    provider.close()
  }
}

case class ConnectorDefaultCredentialsProvider() extends ConnectorAwsCredentialsProvider {

  private var providerOpt: Option[RetryableDefaultCredentialsProvider] = None
  override def provider: AwsCredentialsProvider = {
    if (providerOpt.isEmpty) {
      providerOpt = Some(RetryableDefaultCredentialsProvider())
    }
    providerOpt.get
  }

  override def close(): Unit = {
      providerOpt.foreach(_.close())
  }
}


class Builder {
  def build(): ConnectorAwsCredentialsProvider = {
      ConnectorDefaultCredentialsProvider()
    }
}

object ConnectorAwsCredentialsProvider {
  def builder: Builder = new Builder
}

