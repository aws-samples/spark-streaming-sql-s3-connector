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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import pt.spark.sql.streaming.connector.DataConsumer.CONSUMER_MAX_FILES_PER_TRIGGER
import pt.spark.sql.streaming.connector.DataGenerator.{DATA_PREFIX, testSchemaWithPartition}

// An test consumer using Spark's default file readStream
object FileSourceConsumer {

  def main(args: Array[String]): Unit = {
    val dataSrc = args(0)
    val fileFormat = args(1)
    val checkpointDir = args(2)
    val writeToDir = args(3)
    val readFrom = s"${dataSrc}/${DATA_PREFIX}"

    val spark = SparkSession.builder()
      .appName("FileSourceConsumer")
      .config("spark.sql.ui.explainMode", "extended")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val inputDf = spark
      .readStream
      .format(fileFormat)
      .schema(testSchemaWithPartition)
      .option("maxFilesPerTrigger", CONSUMER_MAX_FILES_PER_TRIGGER)
      .option("region", "us-east-2")
      .load(readFrom)

    val query = inputDf
      .writeStream
      .queryName("FileSourceDataConsumer")
      .format("csv")
      .option("path", writeToDir)
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    query.awaitTermination()
  }
}
