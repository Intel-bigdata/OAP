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

package org.apache.spark.sql.execution.datasources.oap.io

import java.io.Closeable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.column.Dictionary
import org.apache.parquet.hadoop.{IndexedVectorizedOapRecordReader, RecordReaderBuilder, VectorizedOapRecordReader}
import org.apache.parquet.hadoop.api.RecordReader

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper
import org.apache.spark.sql.types.StructType


private[oap] case class ParquetDataFile(path: String,
                                        schema: StructType,
                                        configuration: Configuration,
                                        context: Option[VectorizedContext] = None)
  extends DataFile {

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): FiberCache = {
    // TODO data cache
    throw new UnsupportedOperationException("Not support getFiberData Operation.")
  }

  override def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow] = {

    context match {
      case  Some(c) =>
        addRequestSchemaToConf(conf, requiredIds)
        val file = new Path(StringUtils.unEscapeString(path))
        val meta: ParquetDataFileHandle = DataFileHandleCacheManager(this)
        val reader = new VectorizedOapRecordReader(file, configuration, meta.footer)
        reader.initialize()
        reader.initBatch(c.partitionColumns, c.partitionValues)
        if (c.returningBatch) {
          reader.enableReturningBatches()
        }
        val iter = new FileRecordReaderIterator(reader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
        iter.asInstanceOf[Iterator[InternalRow]]
      case _ =>
        val recordReader = recordReaderBuilder(conf, requiredIds)
          .buildDefault()
        recordReader.initialize()
        new FileRecordReaderIterator[InternalRow](
          recordReader.asInstanceOf[RecordReader[InternalRow]])
    }
  }

  override def iterator(conf: Configuration,
               requiredIds: Array[Int],
               rowIds: Array[Int]): Iterator[InternalRow] = {
    if (rowIds == null || rowIds.length == 0) {
      Iterator.empty
    } else {
      context match {
        case  Some(c) =>
          addRequestSchemaToConf(conf, requiredIds)
          val file = new Path(StringUtils.unEscapeString(path))
          val meta: ParquetDataFileHandle = DataFileHandleCacheManager(this)
          val reader =
            new IndexedVectorizedOapRecordReader(file, configuration, meta.footer, rowIds)
          reader.initialize()
          reader.initBatch(c.partitionColumns, c.partitionValues)
          if (c.returningBatch) {
            reader.enableReturningBatches()
          }
          val iter = new FileRecordReaderIterator(reader)
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
          iter.asInstanceOf[Iterator[InternalRow]]
        case _ =>
          val recordReader = recordReaderBuilder(conf, requiredIds)
            .withGlobalRowIds(rowIds).buildIndexed()
          recordReader.initialize()
          new FileRecordReaderIterator[UnsafeRow](
            recordReader.asInstanceOf[RecordReader[UnsafeRow]])
      }
    }
  }

  private def addRequestSchemaToConf(conf: Configuration, requiredIds: Array[Int]): Unit = {
    val requestSchemaString = {
      var requestSchema = new StructType
      for (index <- requiredIds) {
        requestSchema = requestSchema.add(schema(index))
      }
      requestSchema.json
    }
    conf.set(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaString)
  }

  private def recordReaderBuilder(conf: Configuration,
                                  requiredIds: Array[Int]): RecordReaderBuilder[UnsafeRow] = {
    addRequestSchemaToConf(conf, requiredIds)
    val readSupport = new OapReadSupportImpl
    val meta: ParquetDataFileHandle = DataFileHandleCacheManager(this)
    RecordReaderBuilder
      .builder(readSupport, new Path(StringUtils.unEscapeString(path)), conf)
      .withFooter(meta.footer)
  }

  private class FileRecordReaderIterator[V](private[this] var rowReader: RecordReader[V])
    extends Iterator[V] with Closeable {
    private[this] var havePair = false
    private[this] var finished = false

    override def hasNext: Boolean = {
      if (!finished && !havePair) {
        finished = !rowReader.nextKeyValue
        if (finished) {
          close()
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

    override def close(): Unit = {
      if (rowReader != null) {
        try {
          rowReader.close()
        } finally {
          rowReader = null
        }
      }
    }
  }

  override def createDataFileHandle(): ParquetDataFileHandle = {
    new ParquetDataFileHandle().read(configuration, new Path(StringUtils.unEscapeString(path)))
  }

  override def getDictionary(fiberId: Int, conf: Configuration): Dictionary = null
}
