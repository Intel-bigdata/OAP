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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc._
import org.apache.orc.impl.{ReaderImpl, RecordReaderCacheImpl}
import org.apache.orc.mapred.{OrcInputFormat, OrcStruct}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.orc.{OrcMapreduceRecordReader, _}
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

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
  val meta =
    OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[OrcDataFileMeta]
//  meta.getOrcFileReader()
  private val fileReader: Reader = {
    import scala.collection.JavaConverters._
//    val meta =
//      OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[OrcDataFileMeta]
    meta.getOrcFileReader()
  }

//  recordReader = reader.rows(options)

  val reader: Reader = OrcFile.createReader(meta.path, OrcFile.readerOptions(meta.configuration)
    .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(meta.configuration)).filesystem(meta.fs))
  val options: Reader.Options = OrcInputFormat.buildOptions(meta.configuration,
    reader, 0, meta.length)

  var recordReader: RecordReaderCacheImpl = _

  private val inUseFiberCache = new Array[FiberCache](schema.length)

  private def release(idx: Int): Unit = Option(inUseFiberCache(idx)).foreach { fiberCache =>
    fiberCache.release()
    inUseFiberCache.update(idx, null)
  }

  private[oap] def update(idx: Int, fiberCache: FiberCache): Unit = {
    release(idx)
    inUseFiberCache.update(idx, fiberCache)
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
      new FileRecordReaderIterator[OrcStruct](reader)
    new OapCompletionIterator[OrcStruct](iterator, {}) {
      override def close(): Unit = iterator.close()
    }
  }

  override def totalRows(): Long =
    fileReader.getNumberOfRows()

  override def getDataFileMeta(): DataFileMeta =
    new OrcDataFileMeta(filePath, configuration)

  override def cache(groupId: Int, fiberId: Int): FiberCache = {
    throw new OapException("VectorCache on orc file has been deprecated," +
      " we should never reach here!")
  }
}
