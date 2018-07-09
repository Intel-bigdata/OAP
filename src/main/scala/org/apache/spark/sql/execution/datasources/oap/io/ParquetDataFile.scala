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
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.api.RecordReader
import org.apache.parquet.hadoop.metadata.{IndexedBlockMetaData, OrderedBlockMetaData}

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportWrapper
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, ColumnVectorUtils, OnHeapColumnVector, OnHeapColumnVectorFiber}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

/**
 * ParquetDataFile use xxRecordReader read Parquet Data File,
 * RecordReader divided into 2 categories:
 * <p><b>Vectorized Record Reader</b></p>
 * <ol>
 *   <li><p><b>SpecificOapRecordReaderBase:</b> base of Vectorized Record Reader, similar to
 *     SpecificParquetRecordReaderBase, include initialize and close method.</p></li>
 *   <li><p><b>VectorizedOapRecordReader:</b> extends SpecificOapRecordReaderBase, similar to
 *   VectorizedParquetRecordReader, use for full table scan, support batchReturn feature.</p></li>
 *   <li><p><b>IndexedVectorizedOapRecordReader:</b> extends VectorizedOapRecordReader, use for
 *   indexed table scan, mark valid records in result ColumnarBatch.</p></li>
 *   <li><p><b>SingleGroupOapRecordReader:</b> extends VectorizedOapRecordReader, only read
 *   one RowGroup data of one column, use for load data into DataFiber.</p></li>
 * </ol>
 * <p><b>MapReduce Record Reader</b></p>
 * <ol>
 *   <li><p><b>MrOapRecordReader:</b> similar to ParquetRecordReader of parquet-hadoop module,
 *   use for full table scan, it slow than VectorizedOapRecordReader, but can read all data types
 *   not just AtomicType data.</p></li>
 *   <li><p><b>IndexedMrOapRecordReader:</b> use for indexed table scan, only return row data
 *   in rowIds, it slow than IndexedVectorizedOapRecordReader, but can read all data types
 *   not just AtomicType data.</p></li>
 * </ol>
 * @param path data file path
 * @param schema parquet data file schema
 * @param configuration hadoop configuration
 */
private[oap] case class ParquetDataFile(
    path: String,
    schema: StructType,
    configuration: Configuration) extends DataFile {

  private var context: Option[VectorizedContext] = None
  private lazy val meta =
    OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[ParquetDataFileMeta]
  private val file = new Path(StringUtils.unEscapeString(path))
  private val parquetDataCacheEnable =
    configuration.getBoolean(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key,
      OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.defaultValue.get)

  private var fiberDataReader: ParquetFiberDataReader = _

  private val inUseFiberCache = new Array[FiberCache](schema.length)

  private def release(idx: Int): Unit = synchronized {
    Option(inUseFiberCache(idx)).foreach { fiberCache =>
      fiberCache.release()
      inUseFiberCache.update(idx, null)
    }
  }

  private def update(idx: Int, fiberCache: FiberCache): Unit = {
    release(idx)
    inUseFiberCache.update(idx, fiberCache)
  }

  def cache(groupId: Int, fiberId: Int): FiberCache = {
    if (fiberDataReader == null) {
      // TODO Is there ParquetFooter enough?
      fiberDataReader =
        ParquetFiberDataReader.open(configuration, file, meta.footer.toParquetMetadata)
    }

    val conf = new Configuration(configuration)
    // setting required column to conf enables us to
    // Vectorized read & cache certain(not all) columns
    addRequestSchemaToConf(conf, Array(fiberId))
    ParquetFiberDataLoader(conf, fiberDataReader, groupId).loadSingleColumn
  }

  private def buildIterator(
       conf: Configuration,
       requiredColumnIds: Array[Int],
       context: VectorizedContext,
       rowIds: Option[Array[Int]] = None): OapCompletionIterator[InternalRow] = {
    var requestSchema = new StructType
    for (index <- requiredColumnIds) {
      requestSchema = requestSchema.add(schema(index))
    }

    if (context.partitionColumns != null) {
      for (f <- context.partitionColumns.fields) {
        requestSchema = requestSchema.add(f)
      }
    }

    val iterator = rowIds match {
      case Some(ids) => buildIndexedIterator(conf,
        requiredColumnIds, requestSchema, context, ids)
      case None => buildFullScanIterator(conf,
        requiredColumnIds, requestSchema, context)
    }
    new OapCompletionIterator[InternalRow](iterator, requiredColumnIds.foreach(release)) {
      override def close(): Unit = {
        // To ensure if any exception happens, caches are still released after calling close()
        inUseFiberCache.indices.foreach(release)
        if (fiberDataReader != null) {
          fiberDataReader.close()
        }
      }
    }
  }

  def iterator(
    requiredIds: Array[Int],
    filters: Seq[Filter] = Nil): OapCompletionIterator[InternalRow] = {
    context match {
      case Some(c) =>
        // Parquet RowGroupCount can more than Int.MaxValue,
        // in that sence we should not cache data in memory
        // and rollback to read this rowgroup from file directly.
        if (parquetDataCacheEnable &&
          !meta.footer.getBlocks.asScala.exists(_.getRowCount > Int.MaxValue)) {
          buildIterator(configuration, requiredIds, c)
        } else {
          addRequestSchemaToConf(configuration, requiredIds)
          initVectorizedReader(c,
            new VectorizedOapRecordReader(file, configuration, meta.footer))
        }
      case _ =>
        addRequestSchemaToConf(configuration, requiredIds)
        initRecordReader(
          new MrOapRecordReader[UnsafeRow](new ParquetReadSupportWrapper,
            file, configuration, meta.footer))
    }
  }

  def iteratorWithRowIds(
      requiredIds: Array[Int],
      rowIds: Array[Int],
      filters: Seq[Filter] = Nil): OapCompletionIterator[InternalRow] = {
    if (rowIds == null || rowIds.length == 0) {
      new OapCompletionIterator(Iterator.empty, {})
    } else {
      context match {
        case Some(c) =>
          // Parquet RowGroupCount can more than Int.MaxValue,
          // in that sence we should not cache data in memory
          // and rollback to read this rowgroup from file directly.
          if (parquetDataCacheEnable &&
            !meta.footer.getBlocks.asScala.exists(_.getRowCount > Int.MaxValue)) {
            buildIterator(configuration, requiredIds, c, Some(rowIds))
          } else {
            addRequestSchemaToConf(configuration, requiredIds)
            initVectorizedReader(c,
              new IndexedVectorizedOapRecordReader(file,
                configuration, meta.footer, rowIds))
          }
        case _ =>
          addRequestSchemaToConf(configuration, requiredIds)
          initRecordReader(
            new IndexedMrOapRecordReader[UnsafeRow](new ParquetReadSupportWrapper,
              file, configuration, rowIds, meta.footer))
      }
    }
  }

  def setVectorizedContext(context: Option[VectorizedContext]): Unit =
    this.context = context

  private def initRecordReader(reader: RecordReader[UnsafeRow]) = {
    reader.initialize()
    val iterator = new FileRecordReaderIterator[UnsafeRow](reader)
    new OapCompletionIterator[InternalRow](iterator, {}) {
      override def close(): Unit = iterator.close()
    }
  }

  private def initVectorizedReader(c: VectorizedContext, reader: VectorizedOapRecordReader) = {
    reader.initialize()
    reader.initBatch(c.partitionColumns, c.partitionValues)
    if (c.returningBatch) {
      reader.enableReturningBatches()
    }
    val iterator = new FileRecordReaderIterator(reader)
    new OapCompletionIterator[InternalRow](iterator.asInstanceOf[Iterator[InternalRow]], {}) {
      override def close(): Unit = iterator.close()
    }
  }

  private def buildFullScanIterator(
      conf: Configuration,
      requiredColumnIds: Array[Int],
      requestSchema: StructType,
      context: VectorizedContext): Iterator[InternalRow] = {
    val footer = meta.footer.toParquetMetadata
    footer.getBlocks.asScala.iterator.flatMap { rowGroupMeta =>
      val orderedBlockMetaData = rowGroupMeta.asInstanceOf[OrderedBlockMetaData]
      val columnarBatch = buildColumnarBatchFromCache(orderedBlockMetaData,
        conf, requestSchema, requiredColumnIds, context)
      if (context.returningBatch) {
        Array(columnarBatch).iterator.asInstanceOf[Iterator[InternalRow]]
      } else {
        columnarBatch.rowIterator().asScala
      }
    }
  }

  private def buildIndexedIterator(
      conf: Configuration,
      requiredColumnIds: Array[Int],
      requestSchema: StructType,
      context: VectorizedContext,
      rowIds: Array[Int]): Iterator[InternalRow] = {
    val footer = meta.footer.toParquetMetadata(rowIds)
    footer.getBlocks.asScala.iterator.flatMap { rowGroupMeta =>
      val indexedBlockMetaData = rowGroupMeta.asInstanceOf[IndexedBlockMetaData]
      val columnarBatch = buildColumnarBatchFromCache(indexedBlockMetaData,
        conf, requestSchema, requiredColumnIds, context)
      if (context.returningBatch) {
        columnarBatch.markAllFiltered()
        indexedBlockMetaData.getNeedRowIds.iterator.
          asScala.foreach(rowId => columnarBatch.markValid(rowId))
        Array(columnarBatch).iterator.asInstanceOf[Iterator[InternalRow]]
      } else {
        indexedBlockMetaData.getNeedRowIds.iterator.
          asScala.map(rowId => columnarBatch.getRow(rowId))
      }
    }
  }

  private def buildColumnarBatchFromCache(
      blockMetaData: OrderedBlockMetaData,
      conf: Configuration,
      requestSchema: StructType,
      requiredColumnIds: Array[Int],
      context: VectorizedContext): ColumnarBatch = {
    val groupId = blockMetaData.getRowGroupId
    val fiberCacheGroup = requiredColumnIds.map { id =>
      val fiberCache =
        OapRuntime.getOrCreate.fiberCacheManager.get(DataFiber(this, id, groupId))
      update(id, fiberCache)
      fiberCache
    }
    val rowCount = blockMetaData.getRowCount.toInt
    val columnarBatch = ColumnarBatch.allocate(requestSchema, MemoryMode.ON_HEAP, rowCount)
    columnarBatch.setNumRows(rowCount)
    // populate partitionColumn values
    if (context.partitionColumns != null) {
      val partitionIdx = requiredColumnIds.length
      for (i <- context.partitionColumns.fields.indices) {
        ColumnVectorUtils.populate(columnarBatch.column(i + partitionIdx),
          context.partitionValues, i)
        columnarBatch.column(i + partitionIdx).setIsConstant()
      }
    }
    fiberCacheGroup.zipWithIndex.foreach { case (fiberCache, id) =>
      new OnHeapColumnVectorFiber(
        columnarBatch.column(id).asInstanceOf[OnHeapColumnVector],
        rowCount,
        requestSchema(id).dataType).loadBytesFromCache(fiberCache.getBaseOffset)
    }
    columnarBatch
  }

  private def addRequestSchemaToConf(conf: Configuration, requiredIds: Array[Int]): Unit = {
    val requestSchemaString = {
      var requestSchema = new StructType
      for (index <- requiredIds) {
        requestSchema = requestSchema.add(schema(index))
      }
      requestSchema.json
    }
    conf.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaString)
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

  override def getDataFileMeta(): ParquetDataFileMeta =
    ParquetDataFileMeta(configuration, path)

  override def totalRows(): Long = {
    import scala.collection.JavaConverters._
    val meta =
      OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[ParquetDataFileMeta]
    meta.footer.getBlocks.asScala.foldLeft(0L) {
      (sum, block) => sum + block.getRowCount
    }
  }
}
