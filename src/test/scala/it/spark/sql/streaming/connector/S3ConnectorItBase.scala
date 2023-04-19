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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamTest, StreamingQuery}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File

trait S3ConnectorItBase extends StreamTest{
  val SOURCE_SHORT_NAME: String = "s3-connector"
  val TEST_SPARK_PARTITION = 2
  val TEST_MAX_FILES_PER_TRIGGER = 5

  def uploadS3Path: String
  def testRegion: String

  val testSchema: StructType = StructType(Array(
    StructField("testString", StringType, nullable = true),
    StructField("testBoolean", BooleanType, nullable = true),
    StructField("testInt", IntegerType, nullable = true)
  ))

  val testRawData: Seq[Row] = Seq(
    Row("James", true, 3000),
    Row("Michael", false, 5000),
    Row("Robert", false, 5000)
  )

  val testSchemaWithPartition: StructType = StructType(Array(
    StructField("testString", StringType, nullable = true),
    StructField("testBoolean", BooleanType, nullable = true),
    StructField("testInt", IntegerType, nullable = true),
    StructField("testPart1", StringType, nullable = false),
    StructField("testPart2", IntegerType, nullable = false)
  ))

  val testRawDataWithPartition: Seq[Row] = Seq(
    Row("James", true, 3000, "p1", 1),
    Row("Michael", false, 5000, "p1", 1),
    Row("Robert", false, 5000, "p1", 2),
    Row("James2", true, 3000, "p2", 1),
    Row("Michael2", false, 5000, "p2", 1),
    Row("Robert2", false, 5000, "p2", 3),
  )

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.ui.explainMode", "extended")
      .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
  }


  protected def getDefaultOptions(fileFormat: String): Map[String, String]

  protected def getTestDataFrame(data: Seq[Row], schema: StructType): DataFrame = {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema)

    df
  }

  def createTestCSVFiles(parentDir: File): String = {
    val df = getTestDataFrame(testRawData, testSchema)

    val saveTo = s"${parentDir}/datacsv"
    df.coalesce(TEST_SPARK_PARTITION).write.csv(saveTo)
    saveTo
  }

  def createTestCSVFilesWithHeader(parentDir: File, sep: String): String = {
    val df = getTestDataFrame(testRawData, testSchema)

    val saveTo = s"${parentDir}/datacsv"
    df.coalesce(TEST_SPARK_PARTITION)
      .write
      .option("header", true)
      .option("sep", sep)
      .csv(saveTo)
    saveTo
  }

  def createTestCSVFilesWithPartition(parentDir: File): String = {
    val df = getTestDataFrame(testRawDataWithPartition, testSchemaWithPartition)

    val saveTo = s"${parentDir}/datacsv"
    df.repartition(TEST_SPARK_PARTITION, col("testPart1"), col("testPart2"))
      .write
      .partitionBy("testPart1", "testPart2")
      .csv(saveTo)
    saveTo
  }

  def createTestParquetFiles(parentDir: File): String = {
    val df = getTestDataFrame(testRawData, testSchema)

    val saveTo = s"${parentDir}/datacsv"
    df.coalesce(TEST_SPARK_PARTITION).write.parquet(saveTo)
    saveTo
  }

  def createTestJsonFiles(parentDir: File): String = {
    val df = getTestDataFrame(testRawData, testSchema)

    val saveTo = s"${parentDir}/datacsv"
    df.coalesce(TEST_SPARK_PARTITION).write.json(saveTo)
    saveTo
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  def assertDF(df: DataFrame, expectedAnswer: Seq[Row], trim: Boolean = false): Unit = {
    val isSorted = df.queryExecution.logical.collect { case s: logical.Sort => s }.nonEmpty
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case s: String if trim => s.trim()
          case o => o
        })
      }
      if (!isSorted) converted.sortBy(_.toString()) else converted
    }
    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        fail(errorMessage)
    }

    val prepExpectedAnswer = prepareAnswer(expectedAnswer)
    val prepSparkAnswer = prepareAnswer(sparkAnswer)

    if (prepExpectedAnswer != prepSparkAnswer) {
      val errorMessage =
        s"""
           |Results do not match for query:
           |${df.queryExecution}
           |== Results ==
           |${sideBySide(
          s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepExpectedAnswer.map(_.toString()),
          s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepSparkAnswer.map(_.toString())).mkString("\n")}
      """.stripMargin
      fail(errorMessage)
    }
  }

  private def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.length).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map {
      case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
    }
  }

  def waitForQueryStarted(query: StreamingQuery): Unit = {

    while (!query.isActive) {
      Thread.sleep(1000)
    }
    Thread.sleep(3000)
  }
}
