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

import scala.collection.JavaConverters._

import com.codahale.metrics.{Counter, MetricRegistry}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

class AsyncSqsClientMetricsImpl extends AsyncClientMetrics{

  private val metricRegistry = new MetricRegistry

  private def getCounter(name: String): Counter = {
    metricRegistry.counter(MetricRegistry.name("AsyncSqsClient", name))
  }

  override val receiveMessageCounter: Counter = getCounter("receiveMessageCounter")

  override val receiveMessageFailedCounter: Counter = getCounter("receiveMessageFailedCounter")

  override val parseMessageCounter: Counter = getCounter("parseMassageCounter")

  override val parseMessageFailedCounter: Counter = getCounter("parseMessageFailedCounter")

  override val discardedMessageCounter: Counter = getCounter("discardedMessageCounter")

  override val consumeMessageCounter: Counter = getCounter("consumeMessageCounter")

  override val consumeMessageFailedCounter: Counter = getCounter("consumeMessageFailedCounter")

  override val deleteMessageCounter: Counter = getCounter("deleteMessageCounter")

  override val deleteMessageFailedCounter: Counter = getCounter("deleteMessageFailedCounter")

  override val setMessageVisibilityCounter: Counter = getCounter("setMessageVisibilityCounter")

  override val setMessageVisibilityFailedCounter: Counter = getCounter("setMessageVisibilityFailedCounter")

  override val fetchThreadConsumeMessageCounter: Counter = getCounter("fetchThreadConsumeMessageCounter")

  override val fetchThreadConsumeMessageFailedCounter: Counter = getCounter("fetchThreadConsumeMessageFailedCounter")

  override val fetchThreadUncaughtExceptionCounter: Counter = getCounter("fetchThreadUncaughtExceptionCounter")

  override def json: String = {
    Serialization.write(
      metricRegistry.getCounters.asScala.map { kv =>
        (kv._1, kv._2.getCount)
      }
    )(AsyncSqsClientMetricsImpl.format)
  }
}

object AsyncSqsClientMetricsImpl {
  val format = Serialization.formats(NoTypeHints)

  def apply(): AsyncClientMetrics = {
    new AsyncSqsClientMetricsImpl()
  }
}
