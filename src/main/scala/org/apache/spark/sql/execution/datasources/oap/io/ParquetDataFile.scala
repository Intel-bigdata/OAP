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
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.api.RecordReader
import org.apache.parquet.hadoop.metadata.{IndexedBlockMetaData, OrderedBlockMetaData}

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportWrapper
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, ColumnVectorUtils}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

private[oap] case class ParquetDataFile(
    path: String,
    schema: StructType,
    configuration: Configuration) extends DataFile {

  private var context: Option[VectorizedContext] = None
  private lazy val meta = DataFileMetaCacheManager(this).asInstanceOf[ParquetDataFileMeta]
  private val file = new Path(StringUtils.unEscapeString(path))
  private val parquetDataCacheEnable =
    configuration.getBoolean(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key,
      OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.defaultValue.get)
  private var inputStream: FSDataInputStream = _

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
  def getFiberData(groupId: Int, fiberId: Int): FiberCache = {
    var reader: SingleGroupOapRecordReader = null
    if (inputStream == null) {
      inputStream = file.getFileSystem(configuration).open(file)
    }
    try {
      val conf = new Configuration(configuration)
      val rowGroupRowCount = meta.footer.getBlocks.get(groupId).getRowCount.toInt
      // read a single column for each group.
      // comments: Parquet vectorized read can get multi-columns every time. However,
      // the minimum unit of cache is one column of one group.
      addRequestSchemaToConf(conf, Array(fiberId))
      reader = new SingleGroupOapRecordReader(file, conf, meta.footer, groupId, rowGroupRowCount)
      reader.initialize(inputStream)
      reader.initBatch()
      reader.enableReturningBatches()
      val unitLength = schema(fiberId).dataType match {
        case ByteType | BooleanType => 2
        case ShortType => 3
        case IntegerType | DateType | FloatType => 5
        case LongType | DoubleType => 9
        case _ => -1
      }
      var fiberCache: FiberCache = null

      val nativeAddress = if (unitLength != -1) {
        fiberCache = OapRuntime.getOrCreate.memoryManager.
          getEmptyDataFiberCache(rowGroupRowCount * unitLength)
        // reader.setNativeAddress(fiberCache.getBaseOffset)
        fiberCache.getBaseOffset
      } else {
        -1
      }

      if (reader.nextBatch()) {
          val data = reader.getCurrentValue.asInstanceOf[ColumnarBatch].column(0).
            dumpBytes(nativeAddress)
          if (data != null) {
            OapRuntime.getOrCreate.memoryManager.toDataFiberCache(data)
          } else {
            fiberCache
          }
      } else {
        throw new OapException("buildFiberByteData never reach to here!")
      }
    } finally {
      if (reader != null) {
        try {
          reader.close()
        } finally {
          reader = null
        }
      }
    }
  }

  private def buildIterator(
       conf: Configuration,
       requiredColumnIds: Array[Int],
       context: VectorizedContext,
       rowIds: Option[Array[Int]] = None): OapIterator[InternalRow] = {
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

    new OapIterator[InternalRow](iterator) {
      override def close(): Unit = {
        // To ensure if any exception happens, caches are still released after calling close()
        inUseFiberCache.indices.foreach(release)
        if (inputStream != null) {
          inputStream.close()
        }
      }
    }
  }

  def iterator(requiredIds: Array[Int], filters: Seq[Filter] = Nil): OapIterator[InternalRow] = {
    addRequestSchemaToConf(configuration, requiredIds)
    context match {
      case Some(c) =>
        // Parquet RowGroupCount can more than Int.MaxValue,
        // in that sence we should not cache data in memory
        // and rollback to read this rowgroup from file directly.
        if (parquetDataCacheEnable &&
          !meta.footer.getBlocks.asScala.exists(_.getRowCount > Int.MaxValue)) {
          buildIterator(configuration, requiredIds, c)
        } else {
          initVectorizedReader(c,
            new VectorizedOapRecordReader(file, configuration, meta.footer))
        }
      case _ =>
        initRecordReader(
          new MrOapRecordReader[UnsafeRow](new ParquetReadSupportWrapper,
            file, configuration, meta.footer))
    }
  }

  def iteratorWithRowIds(
      requiredIds: Array[Int],
      rowIds: Array[Int],
      filters: Seq[Filter] = Nil): OapIterator[InternalRow] = {
    if (rowIds == null || rowIds.length == 0) {
      new OapIterator(Iterator.empty)
    } else {
      addRequestSchemaToConf(configuration, requiredIds)
      context match {
        case Some(c) =>
          if (parquetDataCacheEnable) {
            buildIterator(configuration, requiredIds, c, Some(rowIds))
          } else {
            initVectorizedReader(c,
              new IndexedVectorizedOapRecordReader(file,
                configuration, meta.footer, rowIds))
          }
        case _ =>
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
    new OapIterator[InternalRow](iterator) {
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
    new OapIterator[InternalRow](iterator.asInstanceOf[Iterator[InternalRow]]) {
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
      val iter = if (context.returningBatch) {
        Array(columnarBatch).iterator.asInstanceOf[Iterator[InternalRow]]
      } else {
        columnarBatch.rowIterator().asScala
      }
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        iter, requiredColumnIds.foreach(release))
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
      val iter = if (context.returningBatch) {
        columnarBatch.markAllFiltered()
        indexedBlockMetaData.getNeedRowIds.iterator.
          asScala.foreach(rowId => columnarBatch.markValid(rowId))
        Array(columnarBatch).iterator.asInstanceOf[Iterator[InternalRow]]
      } else {
        indexedBlockMetaData.getNeedRowIds.iterator.
          asScala.map(rowId => columnarBatch.getRow(rowId))
      }
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        iter, requiredColumnIds.foreach(release))
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
        OapRuntime.getOrCreate.fiberCacheManager.get(DataFiber(this, id, groupId), conf)
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
      columnarBatch.column(id).loadBytes(fiberCache.getBaseOffset)
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
    val meta = DataFileMetaCacheManager(this).asInstanceOf[ParquetDataFileMeta]
    meta.footer.getBlocks.asScala.foldLeft(0L) {
      (sum, block) => sum + block.getRowCount
    }
  }
}
