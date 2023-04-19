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

import com.codahale.metrics.Counter

trait AsyncClientMetrics {
      def receiveMessageCounter: Counter
      def receiveMessageFailedCounter: Counter
      def parseMessageCounter: Counter
      def parseMessageFailedCounter: Counter
      def discardedMessageCounter: Counter
      def consumeMessageCounter: Counter
      def consumeMessageFailedCounter: Counter
      def deleteMessageCounter: Counter
      def deleteMessageFailedCounter: Counter
      def setMessageVisibilityCounter: Counter
      def setMessageVisibilityFailedCounter: Counter
      def fetchThreadConsumeMessageCounter: Counter
      def fetchThreadConsumeMessageFailedCounter: Counter
      def fetchThreadUncaughtExceptionCounter: Counter
      def json: String
}
