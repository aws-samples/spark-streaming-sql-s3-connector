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

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.amazonaws.spark.sql.streaming.connector.TestUtils.recursiveList
import org.apache.spark.internal.Logging
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{DeleteObjectRequest, ListObjectsV2Request, ListObjectsV2Response, PutObjectRequest}

import java.io.{File, FileInputStream}
import java.net.URI
import java.nio.file.{FileSystems, Files, Path}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asScalaIteratorConverter



object ItTestUtils extends Logging {
  // Set this parameter to false to keep files in S3
  val TEMP_S3_CLEAN_UP: Boolean = false

  def getUploadS3Path: String = {
    Option(System.getenv("TEST_UPLOAD_S3_PATH"))
      .map{timebasedSubDir}
      .getOrElse{
        throw new IllegalArgumentException("Env variable TEST_UPLOAD_S3_PATH " +
          "for uploading test new files must be defined")
      }
  }

  def getTestRegion: String = {
    Option(System.getenv("TEST_REGION")).getOrElse{
      throw new IllegalArgumentException("Env variable TEST_REGION must be defined")
    }
  }

  def getQueueUrl: String = {
    Option(System.getenv("TEST_QUEUE_URL"))
      .getOrElse{
        throw new IllegalArgumentException("Env variable TEST_QUEUE_URL for queue must be defined")
      }
  }

  def getCrossAccountUploadS3Path: String = {
    Option(System.getenv("CROSS_ACCOUNT_TEST_UPLOAD_S3_PATH"))
      .map{timebasedSubDir}
      .getOrElse{
        throw new IllegalArgumentException("Env variable CROSS_ACCOUNT_TEST_UPLOAD_S3_PATH " +
          "for uploading test new files must be defined")
      }
  }

  def getCrossAccountTestRegion: String = {
    Option(System.getenv("CROSS_ACCOUNT_TEST_REGION")).getOrElse{
      throw new IllegalArgumentException("Env variable CROSS_ACCOUNT_TEST_REGION must be defined")
    }
  }

  def getCrossAccountQueueUrl: String = {
    Option(System.getenv("CROSS_ACCOUNT_TEST_QUEUE_URL"))
      .getOrElse{
        throw new IllegalArgumentException("Env variable CROSS_ACCOUNT_TEST_QUEUE_URL for queue must be defined")
      }
  }

  def timebasedSubDir(dir: String): String = {
    if (dir.endsWith("/")) {
      s"${dir}s3dir_${System.currentTimeMillis}/"
    }
    else {
      s"${dir}/s3dir_${System.currentTimeMillis}/"
    }
  }

  def recursiveUploadNewFilesToS3(uploadS3Path: String,
                                  testDataPath: String,
                                  suffix: String,
                                  region: String
                                     ): Unit = {

    val dir = FileSystems.getDefault.getPath(testDataPath).toFile
    val files: java.util.List[File] = recursiveList(dir)
      .filter { file =>
        if (suffix.nonEmpty) file.getName.endsWith(suffix)
        else true
      }
      .toList

    val s3Uri = URI.create(uploadS3Path)
    val bucketName = s3Uri.getHost;
    val prefix = s3Uri.getPath.substring(1);

    val s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    val xfer_mgr = TransferManagerBuilder.standard().withS3Client(s3Client).build()

    try {
      val xfer = xfer_mgr.uploadFileList(bucketName,
      prefix, dir, files);
      xfer.waitForCompletion()
      val progress = xfer.getProgress
      val so_far = progress.getBytesTransferred
      val total = progress.getTotalBytesToTransfer
      val pct = progress.getPercentTransferred
      val xfer_state = xfer.getState
      logInfo(s"xfer_state: ${xfer_state}, so_far: ${so_far}, total: ${total}, pct: ${pct}")
    }
    finally {
      xfer_mgr.shutdownNow()
    }
  }

  def uploadSingleNewFileToS3(uploadS3Path: String, testDataPath: String, suffix: String): Unit = {

    val s3Client: S3Client = S3Client.builder.httpClientBuilder(ApacheHttpClient.builder).build
    try {
      val (bucketName, prefix) = getS3URI(uploadS3Path)

      val dir = FileSystems.getDefault.getPath(testDataPath)

      Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.endsWith(suffix))
        .foreach { f =>
          s3Client.putObject(PutObjectRequest.builder.bucket(bucketName)
            .key(prefix + f.getFileName).build,
            RequestBody.fromBytes(getObjectFile(f)))

          logInfo(s"Upload completed for ${f}")
      }
    }
    finally {
      s3Client.close()
    }
  }

  def removeTempS3FolderIfEnabled(s3Path: String, region: String): Unit = {
    if (TEMP_S3_CLEAN_UP) {
      logInfo(s"Removing files in ${s3Path}")
      val s3Client: S3Client = S3Client.builder
        .httpClientBuilder(ApacheHttpClient.builder)
        .region(Region.of(region))
        .build
      try {
        val (bucketName, prefix) = getS3URI(s3Path)
        var req = ListObjectsV2Request.builder
          .bucket(bucketName)
          .prefix(prefix)
          .build

        var result: ListObjectsV2Response = null
        do {
          result = s3Client.listObjectsV2(req)
          for (s3Object <- result.contents) {
            val deleteObjectRequest = DeleteObjectRequest.builder
              .bucket(bucketName)
              .key(s3Object.key())
              .build
            s3Client.deleteObject(deleteObjectRequest)
          }
          // If there are more than maxKeys keys in the bucket, get a continuation token
          // and list the next objects.
          val token = result.nextContinuationToken
          req = req.toBuilder.continuationToken(token).build
        } while (result.isTruncated)
      }
      finally {
        s3Client.close()
      }
    }

  }

  def getS3URI(s3Path: String): (String, String) = {
    val s3Uri = URI.create(s3Path)
    val bucketName = s3Uri.getHost
    val prefix = s3Uri.getPath.substring(1)
    (bucketName, prefix)
  }

  // Return a byte array.
  private def getObjectFile(filePath: Path) = {

    val file = filePath.toFile
    val bytesArray: Array[Byte] = new Array[Byte](file.length.asInstanceOf[Int])
    var fileInputStream: FileInputStream = null

    try {
      fileInputStream = new FileInputStream(file)
      fileInputStream.read(bytesArray)
    }
    finally if (fileInputStream != null) fileInputStream.close()
    bytesArray
  }

}
