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
package it.spark.sql.streaming.connector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.ForeachWriter

class TestForeachWriter[T](key: String, rowToString: (T) => String) extends ForeachWriter[T] {

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: T): Unit = {
    TestForeachWriter.addValue(key, rowToString(value))
  }

  override def close(errorOrNull: Throwable): Unit = {}

}

object TestForeachWriter {
  private val internalHashMap = new mutable.HashMap[String, ArrayBuffer[String]]()

  def addValue(key: String, value: String): Option[ArrayBuffer[String]] = {
    internalHashMap.synchronized {
      val values = internalHashMap.getOrElse(key, new ArrayBuffer[String]())
      values.append(value)
      internalHashMap.put(key, values)
    }
  }

  def getValues(key: String): ArrayBuffer[String] = internalHashMap.getOrElse(key, ArrayBuffer.empty)

  def allValues: mutable.HashMap[String, ArrayBuffer[String]] = internalHashMap

  def clearAll(): Unit = {
    internalHashMap.synchronized {
      internalHashMap.clear()
    }
  }
}