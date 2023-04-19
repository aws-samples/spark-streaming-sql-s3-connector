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

import scala.util.Random

import pt.spark.sql.streaming.connector.DataConsumer.addConfigForLocalTest

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object DataGenerator {

  val testSchemaWithPartition: StructType = StructType(Array(
    StructField("valString", StringType, nullable = true),
    StructField("valBoolean", BooleanType, nullable = true),
    StructField("valDouble", DoubleType, nullable = true),
    StructField("valInt", IntegerType, nullable = true),
    StructField("valPartition", StringType, nullable = false),
  ))

  val DATA_PREFIX = "datacsv"

  def main(args: Array[String]): Unit = {

    val dataDest = args(0)
    val rowCount: Int = args(1).toInt
    val sparkPartitionCount: Int = args(2).toInt
    val partitionPrefix: String = args(3)

    val sparkBuilder = SparkSession.builder()
      .appName("S3ConnectorPTDataGenerator")
      .config("spark.sql.ui.explainMode", "extended")
    addConfigForLocalTest(sparkBuilder)

    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    try {
      val df = generateDataFrame(spark, rowCount, sparkPartitionCount,
        partitionPrefix, testSchemaWithPartition)

      val saveTo = s"${dataDest}/${DATA_PREFIX}"
      df.repartition(sparkPartitionCount, col("valPartition"))
        .write
        .mode("append")
        .partitionBy("valPartition")
        .csv(saveTo)
    } finally {
      spark.stop()
    }


  }

  def generateDataFrame(spark: SparkSession,
  rowCount: Int,
  partitionCount: Int,
  partitionPrefix: String,
  schema: StructType): DataFrame = {
    def randomString(len: Int) = Random.alphanumeric.take(len).mkString

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq.fill(rowCount) {
          Row(
            randomString(10),
            Random.nextBoolean(),
            Random.nextDouble(),
            Random.nextInt(),
            partitionPrefix + "_" + Random.nextInt(partitionCount).toString)
        }
      ),
      schema
    )

    df
  }

}
