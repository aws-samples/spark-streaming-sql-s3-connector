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

import java.io.File
import java.util.concurrent.{ExecutorService, ScheduledExecutorService, ThreadPoolExecutor}

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.{ThreadUtils, Utils}





object S3SparkUtils {
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(threadName)
  }

  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    ThreadUtils.newDaemonSingleThreadExecutor(threadName)
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    ThreadUtils.newDaemonFixedThreadPool(nThreads, prefix)
  }


  def createTempDir(root: String, namePrefix: String): File = {
    Utils.createTempDir(root, namePrefix)
  }

  def deleteRecursively(file: File): Unit = {
    Utils.deleteRecursively(file)
  }

  def getLocalDir(conf: SparkConf): String = {
    Utils.getLocalDir(conf)
  }

  // Dataset.ofRows moved to org.apache.spark.sql.classic.Dataset in Spark 4.0 and takes a classic
  // SparkSession. One source tree builds against both, so look the companion up by name.
  private lazy val datasetCompanion: (AnyRef, java.lang.reflect.Method) = {
    val name = Seq("org.apache.spark.sql.classic.Dataset$", "org.apache.spark.sql.Dataset$").find { n =>
      Try(Class.forName(n)).isSuccess
    }.getOrElse(throw new IllegalStateException("Spark Dataset companion object not found"))
    val cls = Class.forName(name)
    val ofRows = cls.getMethods.find { m =>
      m.getName == "ofRows" && m.getParameterCount == 2 && m.getParameterTypes()(1) == classOf[LogicalPlan]
    }.getOrElse(throw new IllegalStateException(s"ofRows(SparkSession, LogicalPlan) not found in $name"))
    (cls.getField("MODULE$").get(null), ofRows)
  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val (module, method) = datasetCompanion
    try {
      method.invoke(module, sparkSession, logicalPlan).asInstanceOf[DataFrame]
    } catch {
      case e: java.lang.reflect.InvocationTargetException if e.getCause != null => throw e.getCause
    }
  }

}
