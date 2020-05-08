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
package com.intel.oap.spark.sql.execution.datasources.v2.arrow

import scala.collection.JavaConverters._

import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowSQLConf._
import org.apache.arrow.dataset.scanner.ScanOptions

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.arrow.ArrowUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class ArrowPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: ArrowOptions)
    extends FilePartitionReaderFactory {

  private val batchSize = 4096
  private val enableFilterPushDown: Boolean = sqlConf.arrowFilterPushDown

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    // disable row based read
    throw new UnsupportedOperationException
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val path = partitionedFile.filePath
    val factory = ArrowUtils.makeArrowDiscovery(path, options)
    val dataset = factory.finish()
    val filter = if (enableFilterPushDown) {
      ArrowFilters.translateFilters(pushedFilters)
    } else {
      org.apache.arrow.dataset.filter.Filter.EMPTY
    }
    val scanOptions = new ScanOptions(readDataSchema.map(f => f.name).toArray,
      filter, batchSize)
    val scanner = dataset.newScan(scanOptions)
    val itrList = scanner
      .scan()
      .iterator()
      .asScala
      .map(task => task.scan())
      .toList

    val itr = itrList
      .toIterator
      .flatMap(itr => itr.asScala)
      .map(vsr => ArrowUtils.loadVsr(vsr, partitionedFile.partitionValues,
        readPartitionSchema, readDataSchema))

    new PartitionReader[ColumnarBatch] {

      override def next(): Boolean = {
        itr.hasNext
      }

      override def get(): ColumnarBatch = {
        itr.next()
      }

      override def close(): Unit = {
        itrList.foreach(itr => itr.close())
        scanner.close() // todo memory leak?
      }
    }
  }
}
