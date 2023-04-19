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

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions.{QUEUE_FETCH_WAIT_TIMEOUT_SECONDS, QUEUE_REGION, QUEUE_URL, S3_FILE_FORMAT}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging


trait S3ConnectorTestBase extends SparkFunSuite
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfter with Logging{

  val TESTBASE_DEFAULT_FILE_FORMAT = "csv"
  val TESTBASE_DEFAULT_QUEUE_URL = "testurl"
  val TESTBASE_DEFAULT_QUEUE_REGION = "testregion"
  val TESTBASE_DEFAULT_QUEUE_FETCH_WAIT_TIMEOUT = "5"

  val defaultOptionMap = Map(
    S3_FILE_FORMAT -> TESTBASE_DEFAULT_FILE_FORMAT,
    QUEUE_URL -> TESTBASE_DEFAULT_QUEUE_URL,
    QUEUE_REGION -> TESTBASE_DEFAULT_QUEUE_REGION,
    QUEUE_FETCH_WAIT_TIMEOUT_SECONDS -> TESTBASE_DEFAULT_QUEUE_FETCH_WAIT_TIMEOUT
  )
}
