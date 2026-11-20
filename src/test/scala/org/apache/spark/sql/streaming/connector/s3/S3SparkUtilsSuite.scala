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

package org.apache.spark.sql.streaming.connector.s3

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorTestBase
import com.amazonaws.spark.sql.streaming.connector.TestUtils.withTestTempDir
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, empty, include}

class S3SparkUtilsSuite extends S3ConnectorTestBase with SharedSparkSession {

  private val schema = StructType(Seq(StructField("s", StringType), StructField("i", IntegerType)))

  test("resolveRelationNoFileCheck reads existing files and skips the file check") {
    withTestTempDir { dir =>
      val file = new java.io.File(dir, "a.csv")
      java.nio.file.Files.write(file.toPath, "x,1\ny,2\n".getBytes("UTF-8"))
      val missing = new java.io.File(dir, "missing.csv").getAbsolutePath
      val ds = DataSource(spark, paths = Seq(file.getAbsolutePath, missing),
        userSpecifiedSchema = Some(schema), className = "csv")

      // control: with the file check, the missing path fails
      intercept[AnalysisException](ds.resolveRelation(checkFilesExist = true))

      val relation = S3SparkUtils.resolveRelationNoFileCheck(ds)
      relation.schema shouldBe schema
      val df = S3SparkUtils.ofRows(spark, LogicalRelation(relation))
      df.schema shouldBe schema
    }
  }

  test("findNoFileCheckCall picks the shortest overload and its defaults") {
    val (m, defaults) = S3SparkUtils.findNoFileCheckCall(classOf[S3SparkUtilsSuite.TwoOverloads], "resolve")
    m.getParameterCount shouldBe 1
    defaults shouldBe empty

    val (m2, defaults2) = S3SparkUtils.findNoFileCheckCall(classOf[S3SparkUtilsSuite.WithDefault], "resolve")
    m2.getParameterCount shouldBe 2
    defaults2.map(_.getName) shouldBe Seq("resolve$default$2")
  }

  test("findNoFileCheckCall fails clearly when a parameter has no default") {
    val e = intercept[IllegalStateException](
      S3SparkUtils.findNoFileCheckCall(classOf[S3SparkUtilsSuite.NoDefault], "resolve"))
    e.getMessage should include("parameter 2 has no default")
  }
}

object S3SparkUtilsSuite {
  class TwoOverloads {
    def resolve(check: Boolean): String = "one"
    def resolve(check: Boolean, other: Boolean): String = "two"
  }
  class WithDefault {
    def resolve(check: Boolean = true, readOnly: Boolean = false): String = "x"
  }
  class NoDefault {
    def resolve(check: Boolean, extra: String): String = extra
  }
}
