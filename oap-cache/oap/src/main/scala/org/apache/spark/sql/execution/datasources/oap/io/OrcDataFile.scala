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
import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, VectorizedRowBatch}
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.orc._
import org.apache.orc.impl.{ReaderImpl, RecordReaderCacheImpl}
import org.apache.orc.mapred.{OrcInputFormat, OrcStruct}
import org.apache.orc.mapreduce._
import org.apache.parquet.hadoop.{ParquetFiberDataReader, VectorizedOapRecordReader}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.orc.{OrcMapreduceRecordReader, _}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.oap.OapConf
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
  private lazy val filePath: Path = new Path(path)
  private lazy val orcDataCacheEnable =
    configuration.getBoolean(OapConf.OAP_ORC_DATA_CACHE_ENABLED.key,
      OapConf.OAP_ORC_DATA_CACHE_ENABLED.defaultValue.get)
  lazy val meta =
    OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[OrcDataFileMeta]
//  meta.getOrcFileReader()
  private lazy val fileReader: Reader = {
    import scala.collection.JavaConverters._
//    val meta =
//      OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[OrcDataFileMeta]
    meta.getOrcFileReader()
  }

//  recordReader = reader.rows(options)

  lazy val reader: Reader = OrcFile.createReader(meta.path,
    OrcFile.readerOptions(meta.configuration)
    .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(meta.configuration)).filesystem(meta.fs))
  lazy val options: Reader.Options = OrcInputFormat.buildOptions(meta.configuration,
    reader, 0, meta.length)

  var recordReader: RecordReaderCacheImpl = _

  private lazy val inUseFiberCache = new Array[FiberCache](schema.length)

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
        // Orc Stripe size can more than Int.MaxValue,
        // in that sence we should not cache data in memory
        // and rollback to read this stripe from file directly.
        // TODO support cache without copyToSpark
        if (orcDataCacheEnable &&
          !meta.listStripeInformation.asScala.exists(_.getNumberOfRows > Int.MaxValue &&
          context.copyToSpark)) {
//          addRequestSchemaToConf(configuration, requiredIds)
          initCacheReader(context,
            new OrcCacheReader(configuration,
              meta, this, requiredIds,
              context.enableOffHeapColumnVector, context.copyToSpark))
        } else {
          initVectorizedReader(context,
            new OrcColumnarBatchReader(context.enableOffHeapColumnVector, context.copyToSpark))
        }
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
          // TODO support cache without copyToSpark
          if (orcDataCacheEnable &&
            !meta.listStripeInformation.asScala.exists(_.getNumberOfRows > Int.MaxValue &&
              context.copyToSpark)) {
            //          addRequestSchemaToConf(configuration, requiredIds)
            initCacheReader(context,
              new IndexedOrcCacheReader(configuration,
                meta, this, requiredIds,
                context.enableOffHeapColumnVector, context.copyToSpark, rowIds))
          } else {
            initVectorizedReader(context,
              new IndexedOrcColumnarBatchReader(context.enableOffHeapColumnVector,
                context.copyToSpark, rowIds))
          }
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
    val taskConf = new Configuration(configuration)
    taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
      c.requestedColIds.filter(_ != -1).sorted.mkString(","))
    reader.initialize(filePath, taskConf)
    reader.initBatch(fileReader.getSchema, c.requestedColIds, c.requiredSchema.fields,
      c.partitionColumns, c.partitionValues)
    val iterator = new FileRecordReaderIterator(reader)
    new OapCompletionIterator[InternalRow](iterator.asInstanceOf[Iterator[InternalRow]], {}) {
      override def close(): Unit = iterator.close()
    }
  }

  private def initCacheReader(c: OrcDataFileContext,
      reader: OrcCacheReader) = {
    reader.initialize(filePath, configuration)
    reader.initBatch(fileReader.getSchema, c.requestedColIds, c.requiredSchema.fields,
      c.partitionColumns, c.partitionValues)
    val iterator = new FileRecordReaderIterator(reader)
    // TODO need to release
    new OapCompletionIterator[InternalRow](iterator.asInstanceOf[Iterator[InternalRow]],
      c.requestedColIds.foreach(release)) {
      override def close(): Unit = {
        // To ensure if any exception happens, caches are still released after calling close()
        inUseFiberCache.indices.foreach(release)
        if (recordReader != null) {
          recordReader.close()
        }
      }
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
    val fileSchema = fileReader.getSchema
    val columnTypeDesc = TypeDescription.createStruct()
      .addField(fileSchema.getFieldNames.get(fiberId), fileSchema.getChildren.get(fiberId))
    options.schema(columnTypeDesc)
    recordReader = new RecordReaderCacheImpl(reader.asInstanceOf[ReaderImpl], options)

    val rowCount = meta.listStripeInformation.get(groupId).getNumberOfRows.toInt
    val vectorizedRowBatch = columnTypeDesc.createRowBatch(rowCount)
    recordReader.readStripeByOap(groupId)
    recordReader.readBatch(vectorizedRowBatch)
    recordReader.close()

    val fromColumn = vectorizedRowBatch.cols(0)
    val field = schema.fields(fiberId)
    val toColumn = new OnHeapColumnVector(rowCount, field.dataType)
    if (fromColumn.isRepeating) {
     OrcCacheReader.putRepeatingValues(rowCount, field, fromColumn, toColumn)
    }
    else if (fromColumn.noNulls) {
    OrcCacheReader.putNonNullValues(rowCount, field, fromColumn, toColumn)
    }
    else {
      OrcCacheReader.putValues(rowCount, field, fromColumn, toColumn)
    }
    ParquetDataFiberWriter.dumpToCache(toColumn, rowCount)
  }
}
