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

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.concurrent.{ExecutorService, TimeUnit}

import org.apache.spark.internal.Logging

object Utils extends Logging{

  // TODO: default values configurable
  val DEFAULT_CONNECTION_ACQUIRE_TIMEOUT = 60 // seconds
  val DEFAULT_SHUTDOWN_WAIT_TIMEOUT = 180 // seconds

  def reportTimeTaken[T](operation: String)(body: => T): T = {
    val startTime = System.currentTimeMillis()
    val result = body
    val endTime = System.currentTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    logInfo(s"reportTimeTaken $operation took $timeTaken ms")
    result
  }

  def shutdownAndAwaitTermination(pool: ExecutorService, await_timeout: Int = DEFAULT_SHUTDOWN_WAIT_TIMEOUT): Unit = {
    if (! pool.isTerminated) {
      pool.shutdown() // Disable new tasks from being submitted

      try {
        // Wait a while for existing tasks to terminate
        if (!pool.awaitTermination(await_timeout, TimeUnit.SECONDS)) {
          pool.shutdownNow // Cancel currently executing tasks

          // Wait a while for tasks to respond to being cancelled
          if (!pool.awaitTermination(await_timeout, TimeUnit.SECONDS)) {
            logError(s"Thread pool did not stop properly: ${pool.toString}.")
          }
        }
      } catch {
        case _: InterruptedException =>
          // (Re-)Cancel if current thread also interrupted
          pool.shutdownNow
          // Preserve interrupt status
          Thread.currentThread.interrupt()
      }
    }
  }

  def convertTimestampToMills(timestamp: String): Long = {
    val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
    timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val timeInMillis = timestampFormat.parse(timestamp).getTime
    timeInMillis
  }
}
