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
package pt.spark.sql.streaming.connector

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorFileCache
import com.amazonaws.spark.sql.streaming.connector.metadataLog.RocksDBS3SourceLog
import org.apache.log4j.LogManager
import pt.spark.sql.streaming.connector.DataConsumer.addConfigForLocalTest

import org.apache.spark.sql.SparkSession

object TestTool {

  def main(args: Array[String]): Unit = {
    val command = args(0)

    val log = LogManager.getRootLogger

    val sparkBuilder = SparkSession.builder()
      .appName("TestTool")
      .config("spark.sql.ui.explainMode", "extended")

    addConfigForLocalTest(sparkBuilder)

    val spark = sparkBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    try {
      command match {
        case "PrintRocksDB" =>
          val metadataPath = args(1)
          val fileCache = new S3ConnectorFileCache(Int.MaxValue)

          val metadataLog = new RocksDBS3SourceLog()
          metadataLog.init(spark, metadataPath, fileCache)
          metadataLog.printAllBatchesInRocksDB()
          metadataLog.close()

        case _ => log.error(s"Unknown command: ${command}")

      }
    } finally {
      spark.stop()
    }
  }
}
