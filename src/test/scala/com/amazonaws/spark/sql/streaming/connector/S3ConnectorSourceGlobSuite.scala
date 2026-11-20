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

import java.io.File
import java.nio.file.Files

import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.streaming.connector.s3.S3SparkUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class S3ConnectorSourceGlobSuite extends S3ConnectorTestBase with SharedSparkSession {

  private val schema = StructType(Seq(StructField("v", StringType)))

  // Each key sits next to a file its glob form would also match; only the literal key may be read.
  Seq(
    "a*b.csv" -> "axb.csv",
    "a?b.csv" -> "axb.csv",
    "a[1].csv" -> "a1.csv",
    "a{b,c}.csv" -> "ab.csv"
  ).foreach { case (key, decoy) =>
    test(s"DataSource reads only the literal key '$key'") {
      // file names with * and ? and a "file://" + absolute path need a Unix file system
      assume(File.separatorChar == '/')
      withTestTempDir { dir =>
        Files.write(new File(dir, key).toPath, "literal\n".getBytes("UTF-8"))
        Files.write(new File(dir, decoy).toPath, "decoy\n".getBytes("UTF-8"))
        // decoded form, like s3://bucket/<key> after URLDecoder in the SQS client
        val path = S3ConnectorSource.toDataSourcePath("file://" + new File(dir, key).getAbsolutePath)
        val ds = DataSource(spark, paths = Seq(path), userSpecifiedSchema = Some(schema), className = "csv")
        val df = S3SparkUtils.ofRows(spark, LogicalRelation(S3SparkUtils.resolveRelationNoFileCheck(ds)))
        df.collect().map(_.getString(0)).toSeq shouldBe Seq("literal")
      }
    }
  }
}
