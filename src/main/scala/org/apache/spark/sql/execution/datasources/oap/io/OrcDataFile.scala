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
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.orc._
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.orc.{IndexedOrcColumnarBatchReader, IndexedOrcMapreduceRecordReader, OrcColumnarBatchReader, OrcMapreduceRecordReader}
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

/**
 * OrcDataFile is using below four record readers to read orc data file.
 *
 * For vectorization, one is with oap index while the other is without oap index.
 * The above two readers are OrcColumnarBatchReader and extended
 * IndexedOrcColumnarBatchReader.
 *
 * For no vectorization, similarly one is with oap index while the other is without oap index.
 * The above two readers are OrcMapreduceRecordReader and extended
 * IndexedOrcMapreduceRecordReader.
 *
 * For the option to enable vectorization or not, it's similar to Parquet.
 * All of the above four readers are under
 * src/main/java/org/apache/spark/sql/execution/datasources/oap/orc.
 *
 * @param path data file path
 * @param schema orc data file schema
 * @param configuration hadoop configuration
 */
private[oap] case class OrcDataFile(
    path: String,
    schema: StructType,
    configuration: Configuration) extends DataFile {

  private var context: OrcDataFileContext = _
  private val filePath: Path = new Path(path)
  private val fileReader: Reader = {
    import scala.collection.JavaConverters._
    val meta =
      OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[OrcDataFileMeta]
    meta.getOrcFileReader()
  }

  def iterator(
    requiredIds: Array[Int],
    filters: Seq[Filter] = Nil): OapCompletionIterator[Any] = {
    val iterator = context.returningBatch match {
      case true =>
        initVectorizedReader(context,
          new OrcColumnarBatchReader(context.enableOffHeapColumnVector, context.copyToSpark))
      case false =>
        initRecordReader(
          new OrcMapreduceRecordReader[OrcStruct](filePath, configuration))
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
      val iterator = context.returningBatch match {
        case true =>
          initVectorizedReader(context,
            new IndexedOrcColumnarBatchReader(context.enableOffHeapColumnVector,
              context.copyToSpark, rowIds))
        case false =>
          initRecordReader(
            new IndexedOrcMapreduceRecordReader[OrcStruct](filePath, configuration, rowIds))
      }
      iterator.asInstanceOf[OapCompletionIterator[Any]]
    }
  }

  def setOrcDataFileContext(context: OrcDataFileContext): Unit =
    this.context = context

  private def initVectorizedReader(c: OrcDataFileContext,
      reader: OrcColumnarBatchReader) = {
    reader.initialize(filePath, configuration)
    reader.initBatch(fileReader.getSchema, c.requestedColIds, c.requiredSchema.fields,
      c.partitionColumns, c.partitionValues)
    val iterator = new FileRecordReaderIterator(reader)
    new OapCompletionIterator[InternalRow](iterator.asInstanceOf[Iterator[InternalRow]], {}) {
      override def close(): Unit = iterator.close()
    }
  }

  private def initRecordReader(
      reader: OrcMapreduceRecordReader[OrcStruct]) = {
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

  override def totalRows(): Long =
    fileReader.getNumberOfRows()

  override def getDataFileMeta(): DataFileMeta =
    new OrcDataFileMeta(filePath, configuration)

  // Data cache is not supported, since the performance with cache
  // is worse than without cache for Parquet.
  override def cache(groupId: Int, fiberId: Int): FiberCache = {
    throw new OapException("Data cache is not supported for Orc.\n")
  }
}
