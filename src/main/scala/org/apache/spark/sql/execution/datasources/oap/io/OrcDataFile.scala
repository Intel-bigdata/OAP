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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.orc._
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.orc.{OrcColumnarBatchReaderForOap, OrcColumnarBatchReaderForOapIndex, OrcMapreduceRecordReaderForOap, OrcMapreduceRecordReaderForOapIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CompletionIterator

/**
 * OrcDataFile is using below four record readers to read orc data file.
 *
 * For vectorization, one is with oap index while the other is without oap index.
 * The above two readers are OrcColumnarBatchReaderForOap and extended
 * OrcColumnarBatchReaderForOapIndex.
 *
 * For no vectorization, similarly one is with oap index while the other is without oap index.
 * The above two readers are OrcMapreduceRecordReaderForOap and extended
 * OrcMapreduceRecordReaderForOapIndex.
 *
 * For the option to enable vectorization or not, it's similar to Parquet.
 * All of the above four readers are under
 * src/main/java/org/apache/spark/sql/execution/datasources/orc.
 *
 * @param path data file path
 * @param schema orc data file schema
 * @param configuration hadoop configuration
 */
private[oap] case class OrcDataFile(
    path: String,
    schema: StructType,
    configuration: Configuration) extends DataFile {

  private var context: Option[OrcVectorizedContext] = None
  private val filePath = new Path(path)
  private val fs = filePath.getFileSystem(configuration)
  private val readerOptions = OrcFile.readerOptions(configuration).filesystem(fs)
  // TODO: May need to cache fileReader.
  private val fileReader = OrcFile.createReader(filePath, readerOptions)

  def iterator(
    requiredIds: Array[Int],
    filters: Seq[Filter] = Nil): OapCompletionIterator[Any] = {
    val iterator = context match {
      case Some(c) =>
        initVectorizedReader(c,
          new OrcColumnarBatchReaderForOap(c.enableOffHeapColumnVector, c.copyToSpark))
      case _ =>
        initRecordReader(
          new OrcMapreduceRecordReaderForOap[OrcStruct](filePath, configuration))
    }
    iterator.asInstanceOf[OapCompletionIterator[Any]]
  }

  def iteratorWithRowIds(
      requiredIds: Array[Int],
      rowIds: Array[Int],
      filters: Seq[Filter] = Nil): OapCompletionIterator[Any] = {
    if (rowIds == null || rowIds.length == 0) {
      new OapCompletionIterator(Iterator.empty, {})
    } else {
      val iterator = context match {
        case Some(c) =>
          initVectorizedReader(c,
            new OrcColumnarBatchReaderForOapIndex(c.enableOffHeapColumnVector, c.copyToSpark,
              rowIds))
        case _ =>
          initRecordReader(
            new OrcMapreduceRecordReaderForOapIndex[OrcStruct](filePath, configuration, rowIds))
      }
      iterator.asInstanceOf[OapCompletionIterator[Any]]
    }
  }

  def setVectorizedContext(context: Option[OrcVectorizedContext]): Unit =
    this.context = context

  private def initVectorizedReader(c: OrcVectorizedContext,
      reader: OrcColumnarBatchReaderForOap) = {
    reader.initialize(filePath, configuration)
    reader.initBatch(fileReader.getSchema, c.requestedColIds, c.requiredSchema.fields,
      c.partitionColumns, c.partitionValues)
    val iterator = new FileRecordReaderIterator(reader)
    new OapCompletionIterator[InternalRow](iterator.asInstanceOf[Iterator[InternalRow]], {}) {
      override def close(): Unit = iterator.close()
    }
  }

  private def initRecordReader(
      reader: OrcMapreduceRecordReaderForOap[OrcStruct]) = {
    val iterator =
      new FileRecordReaderIterator[OrcStruct](reader.asInstanceOf[RecordReader[_, OrcStruct]])
    new OapCompletionIterator[OrcStruct](iterator, {}) {
      override def close(): Unit = iterator.close()
    }
  }

  private class FileRecordReaderIterator[V](private[this] var rowReader: RecordReader[_, V])
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

  override def totalRows(): Long = fileReader.getNumberOfRows()

  // Abstract to the common data structure later, since Orc doesn't need file meta.
  override def getDataFileMeta(): DataFileMeta = Nil.asInstanceOf[DataFileMeta]

  // Cache will be added later.
  override def cache(groupId: Int, fiberId: Int): FiberCache = Nil.asInstanceOf[FiberCache]
}
