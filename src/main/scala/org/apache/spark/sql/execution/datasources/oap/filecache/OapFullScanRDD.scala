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

package org.apache.spark.sql.execution.datasources.oap.filecache

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.{InputFileNameHolder, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.io.{DataFile, OapDataFile}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

class OapFullScanRDD(
    @transient private val sparkSession: SparkSession,
    @transient val filePartitions: Seq[FilePartition],
    schema: StructType)
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val files = split.asInstanceOf[FilePartition].files.toIterator
    assert(split.asInstanceOf[FilePartition].files.length == 1, "1 partition : 1 file")

    val conf = new Configuration()
    files.flatMap { file =>
      InputFileNameHolder.setInputFileName(file.filePath)
      val metaPath = new Path(file.filePath).getParent
      val meta = DataSourceMeta.initialize(new Path(metaPath, OapFileFormat.OAP_META_FILE), conf)
      val fileScanner = DataFile(file.filePath, meta.schema, meta.dataReaderClassName, conf)
      val s = meta.schema
      val ids = schema.map { field => s.indexOf(field)}
      fileScanner.asInstanceOf[OapDataFile].iteratorNG(ids.toArray)
    }
  }

  override protected def getPartitions: Array[Partition] = filePartitions.toArray

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val files = split.asInstanceOf[FilePartition].files

    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }
  }
}
