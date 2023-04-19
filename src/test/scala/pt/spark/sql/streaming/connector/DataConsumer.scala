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

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorSourceOptions._
import pt.spark.sql.streaming.connector.DataGenerator.{testSchemaWithPartition, DATA_PREFIX}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object DataConsumer {
  val SOURCE_SHORT_NAME: String = "s3-connector"
  val QUEUE_NAME: String = "PTConsume"
  val CONSUMER_MAX_FILES_PER_TRIGGER: String = "5000"

  val localTest: Boolean = System.getProperty("os.name").toLowerCase().startsWith("mac os")

  def main(args: Array[String]): Unit = {

    val dataSrc = args(0)
    val queueUrl = args(1)
    val fileFormat = args(2)
    val checkpointDir = args(3)
    val writeToDir = args(4)
    val readFrom = s"${dataSrc}/${DATA_PREFIX}"

    val sparkBuilder = SparkSession.builder()
      .appName("S3ConnectorPTDataConsumer")
      .config("spark.sql.ui.explainMode", "extended")

    addConfigForLocalTest(sparkBuilder)

    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val connectorOptions = spark.sqlContext.getAllConfs ++ Map(
      QUEUE_REGION -> "us-east-2",
      S3_FILE_FORMAT -> fileFormat,
      MAX_FILES_PER_TRIGGER -> CONSUMER_MAX_FILES_PER_TRIGGER,
      MAX_FILE_AGE->"15d",
      QUEUE_URL -> queueUrl,
      QUEUE_FETCH_WAIT_TIMEOUT_SECONDS -> "10",
      SQS_LONG_POLLING_WAIT_TIME_SECONDS -> "5",
      SQS_VISIBILITY_TIMEOUT_SECONDS -> "60",
      PATH_GLOB_FILTER -> s"*.${fileFormat}",
      BASE_PATH -> readFrom
    )

    try {
      val inputDf = spark
        .readStream
        .format(SOURCE_SHORT_NAME)
        .schema(testSchemaWithPartition)
        .options(connectorOptions)
        .load()

      val query = inputDf
        .writeStream
        .queryName(QUEUE_NAME)
        .format("csv")
        .option("path", writeToDir)
        .option("checkpointLocation", checkpointDir)
        .trigger(Trigger.ProcessingTime("15 seconds"))
        .start()

      query.awaitTermination()
    } finally {
      spark.stop()
    }

  }

  def addConfigForLocalTest(sparkBuilder: SparkSession.Builder): Unit = {
    if (localTest) {
      sparkBuilder
        .master("local[2]")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
          "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
//        .config("spark.driver.bindAddress", "127.0.0.1") // VPN env.
    }
  }
}
