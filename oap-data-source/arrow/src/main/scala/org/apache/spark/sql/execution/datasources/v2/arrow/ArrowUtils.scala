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
package org.apache.spark.sql.execution.datasources.v2.arrow

import java.net.URI
import java.util.TimeZone

import org.apache.arrow.dataset.file.{FileSystem, SingleFileDataSourceDiscovery}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

object ArrowUtils {
  def readSchema(file: FileStatus, options: CaseInsensitiveStringMap): Option[StructType] = {
    val discovery: SingleFileDataSourceDiscovery =
      makeArrowDiscovery(file.getPath.toString, new ArrowOptions(options.asScala.toMap))
    val schema = discovery.inspect() // todo mem leak?
    Option(org.apache.spark.sql.util.ArrowUtils.fromArrowSchema(schema))
  }

  def readSchema(files: Seq[FileStatus], options: CaseInsensitiveStringMap): Option[StructType] =
    readSchema(files.toList.head, options) // todo merge schema

  def makeArrowDiscovery(file: String, options: ArrowOptions): SingleFileDataSourceDiscovery = {

    val format = getFormat(options).getOrElse(throw new IllegalStateException)
    val fs = getFs(options).getOrElse(throw new IllegalStateException)

    val discovery = new SingleFileDataSourceDiscovery(
      org.apache.spark.sql.util.ArrowUtils.rootAllocator,
      format,
      fs,
      rewriteFilePath(file))
    discovery
  }

  def rewriteFilePath(file: String): String = {
    val uri = URI.create(file)
    if (uri.getScheme == "hdfs") {
      var query = uri.getQuery
      if (query == null) {
        query = "use_hdfs3=1"
      } else {
        query += "&use_hdfs3=1"
      }
      return new URI(uri.getScheme, uri.getAuthority, uri.getPath, query, uri.getFragment).toString
    }
    file
  }

  def toArrowSchema(t: StructType): Schema = {
    // fixme this might be platform dependent
    org.apache.spark.sql.util.ArrowUtils.toArrowSchema(t, TimeZone.getDefault.getID)
  }

  private def getFormat(
    options: ArrowOptions): Option[org.apache.arrow.dataset.file.FileFormat] = {
    Option(options.originalFormat match {
      case "parquet" => org.apache.arrow.dataset.file.FileFormat.PARQUET
      case _ => throw new IllegalArgumentException("Unrecognizable format")
    })
  }

  private def getFs(options: ArrowOptions): Option[FileSystem] = {
    Option(options.filesystem match {
      case "local" => FileSystem.LOCAL
      case "hdfs" => FileSystem.HDFS
      case _ => throw new IllegalArgumentException("Unrecognizable filesystem")
    })
  }
}
