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

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class S3ConnectorSourcePathSuite extends AnyFunSuite {

  // Decoded S3 keys as they come out of the SQS message -> key S3A must read
  Seq(
    "dir/plain.csv",
    "dir/with space.csv",
    "dir/a+b=1,c.csv",
    "dir/100%.csv",
    "dir/literal%41.csv",
    "dir/a:b.csv",
    "dir/ lead.csv",
    "dir/\u00e9.csv"
  ).foreach { key =>
    test(s"toDataSourcePath keeps key '$key'") {
      val dsPath = S3ConnectorSource.toDataSourcePath(s"s3://bucket/$key")
      // DataSource parses the string with Path(String) again
      val uri = new Path(dsPath).toUri
      uri.getScheme shouldBe "s3"
      uri.getAuthority shouldBe "bucket"
      uri.getPath shouldBe s"/$key"
    }
  }

  // Glob characters are escaped with '\' so DataSource's globbing matches only the literal key
  Seq(
    "dir/a*b.csv" -> "dir/a\\*b.csv",
    "dir/a?b.csv" -> "dir/a\\?b.csv",
    "dir/a[1].csv" -> "dir/a\\[1\\].csv",
    "dir/a{b,c}.csv" -> "dir/a\\{b,c\\}.csv",
    "dir/a\\b.csv" -> "dir/a\\\\b.csv"
  ).foreach { case (key, escaped) =>
    test(s"toDataSourcePath escapes glob characters in '$key'") {
      S3ConnectorSource.toDataSourcePath(s"s3://bucket/$key") shouldBe s"s3://bucket/$escaped"
    }
  }

  test("toDataSourcePath normalises '..' (basePath check uses the same normalisation)") {
    S3ConnectorSource.toDataSourcePath("s3://bucket/root/../other/x.csv") shouldBe "s3://bucket/other/x.csv"
  }
}
