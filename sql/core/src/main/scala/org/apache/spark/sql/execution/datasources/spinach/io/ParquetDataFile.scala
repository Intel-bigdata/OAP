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

package org.apache.spark.sql.execution.datasources.spinach.io

import java.lang.{Long => JLong}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.RecordReader
import org.apache.parquet.hadoop.SpinachRecordReader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper
import org.apache.spark.sql.execution.datasources.spinach.filecache.DataFiberCache
import org.apache.spark.sql.types.StructType


private[spinach] case class ParquetDataFile(path: String, schema: StructType) extends DataFile {

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): DataFiberCache = {
    throw new UnsupportedOperationException("Not support getFiberData Operation.")
  }

  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow] = {
    iterator(conf, requiredIds, null)
  }

  def iterator(conf: Configuration,
               requiredIds: Array[Int],
               rowIds: Array[Long]): Iterator[InternalRow] = {
    if (rowIds != null && rowIds.length == 0) {
      new Iterator[UnsafeRow] {
        override def hasNext: Boolean = false

        override def next(): UnsafeRow =
          throw new java.util.NoSuchElementException("Input is Empty RowIds Array")
      }
    } else {
      val requestSchemaString = {
        var requestSchema = new StructType
        for (index <- requiredIds) {
          requestSchema = requestSchema.add(schema(index))
        }
        requestSchema.json
      }
      conf.set(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaString)
      conf.set(SpinachReadSupportImpl.SPARK_ROW_READ_FROM_FILE_SCHEMA, requestSchemaString)

      val readSupport = new SpinachReadSupportImpl

      val recordReader = SpinachRecordReader.builder(readSupport, new Path(path), conf)
        .withGlobalRowIds(rowIds).build()
      recordReader.initialize()
      new FileRecordReaderIterator[JLong, UnsafeRow](
        recordReader.asInstanceOf[RecordReader[JLong, UnsafeRow]])
    }
  }

  private class FileRecordReaderIterator[ID, V](rowReader: RecordReader[ID, V])
    extends Iterator[V] {
    private[this] var havePair = false
    private[this] var finished = false

    override def hasNext: Boolean = {
      if (!finished && !havePair) {
        finished = !rowReader.nextKeyValue
        if (finished) {
          rowReader.close()
        }
        havePair = !finished
      }
      !finished
    }

    override def next(): V = {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      rowReader.getCurrentValue
    }
  }

  override def createDataFileHandle(conf: Configuration): DataFileHandle = {
    throw new UnsupportedOperationException("Not support initialize Operation.")
  }
}
