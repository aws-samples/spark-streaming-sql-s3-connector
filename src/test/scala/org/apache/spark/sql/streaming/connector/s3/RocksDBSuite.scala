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

import java.nio.charset.StandardCharsets.UTF_8

import com.amazonaws.spark.sql.streaming.connector.S3ConnectorTestBase
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.util.Utils

class RocksDBSuite extends S3ConnectorTestBase {

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)

  private def str(b: Array[Byte]): String = if (b == null) null else new String(b, UTF_8)

  private def withDb(f: RocksDB => Unit): Unit = {
    val dfsDir = Utils.createTempDir()
    val db = new RocksDB(dfsDir.getAbsolutePath, RocksDBConf(), Utils.createTempDir())
    try {
      f(db)
    } finally {
      db.close()
      Utils.deleteRecursively(dfsDir)
    }
  }

  test("remove deletes a key that is still in the write batch") {
    withDb { db =>
      db.load(0)
      db.put(bytes("k1"), bytes("v1"))
      db.put(bytes("k2"), bytes("v2"))

      str(db.remove(bytes("k1"))) shouldBe "v1"

      db.get(bytes("k1")) shouldBe null
      str(db.get(bytes("k2"))) shouldBe "v2"
    }
  }

  test("remove deletes a committed key and the removal survives a new commit and reload") {
    withDb { db =>
      db.load(0)
      db.put(bytes("k1"), bytes("v1"))
      db.put(bytes("k2"), bytes("v2"))
      val version1 = db.commit()

      db.load(version1)
      str(db.remove(bytes("k1"))) shouldBe "v1"
      val version2 = db.commit()

      db.load(version2)
      db.get(bytes("k1")) shouldBe null
      str(db.get(bytes("k2"))) shouldBe "v2"
    }
  }

  test("remove of a missing key returns null and does not change the other keys") {
    withDb { db =>
      db.load(0)
      db.put(bytes("k1"), bytes("v1"))

      db.remove(bytes("missing")) shouldBe null

      str(db.get(bytes("k1"))) shouldBe "v1"
    }
  }
}
