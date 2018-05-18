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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.{BatchColumn, ColumnValues}
import org.apache.spark.sql.execution.datasources.oap.filecache.{MemoryManager, _}
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportWrapper
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.internal.oap.OapConf
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
  private lazy val meta = DataFileMetaCacheManager(this).asInstanceOf[ParquetDataFileMeta]
  private val file = new Path(StringUtils.unEscapeString(path))
  private val parquetDataCacheEnable =
    configuration.getBoolean(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key,
      OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.defaultValue.get)

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

  private def buildFiberByteData(
       dataType: DataType,
       rowGroupRowCount: Int,
       reader: SingleGroupOapRecordReader): Array[Byte] = {
    val dataFiberBuilder: DataFiberBuilder = dataType match {
      case StringType =>
        StringFiberBuilder(rowGroupRowCount, 0)
      case BinaryType =>
        BinaryFiberBuilder(rowGroupRowCount, 0)
      case BooleanType | ByteType | DateType | DoubleType | FloatType | IntegerType |
           LongType | ShortType =>
        FixedSizeTypeFiberBuilder(rowGroupRowCount, 0, dataType)
      case _ => throw new NotImplementedError(s"${dataType.simpleString} data type " +
        s"is not implemented for fiber builder")
    }

    if (reader.nextBatch()) {
      while (reader.nextKeyValue()) {
        dataFiberBuilder.append(reader.getCurrentValue.asInstanceOf[ColumnarBatch.Row])
      }
    } else {
      throw new OapException("buildFiberByteData never reach to here!")
    }

    dataFiberBuilder.build().fiberData
  }

  def getFiberData(groupId: Int, fiberId: Int): FiberCache = {
    var reader: SingleGroupOapRecordReader = null
    try {
      val conf = new Configuration(configuration)
      val rowGroupRowCount = meta.footer.getBlocks.get(groupId).getRowCount.toInt
      // read a single column for each group.
      // comments: Parquet vectorized read can get multi-columns every time. However,
      // the minimum unit of cache is one column of one group.
      addRequestSchemaToConf(conf, Array(fiberId))
      reader = new SingleGroupOapRecordReader(file, conf, meta.footer, groupId, rowGroupRowCount)
      reader.initialize()
      reader.initBatch()
      val data = buildFiberByteData(schema(fiberId).dataType, rowGroupRowCount, reader)
      MemoryManager.toDataFiberCache(data)
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
       rowIds: Option[Array[Int]]): OapIterator[InternalRow] = {
    val iterator = rowIds match {
      case Some(ids) => buildIndexedIterator(conf, requiredColumnIds, ids)
      case None => buildFullScanIterator(conf, requiredColumnIds)
    }
    new OapIterator[InternalRow](iterator) {
      override def close(): Unit = {
        // To ensure if any exception happens, caches are still released after calling close()
        inUseFiberCache.indices.foreach(release)
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
          buildIterator(configuration, requiredIds, rowIds = None)
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
            buildIterator(configuration, requiredIds, Some(rowIds))
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
      requiredColumnIds: Array[Int]): Iterator[InternalRow] = {
    val footer = meta.footer.toParquetMetadata
    footer.getBlocks.asScala.iterator.flatMap { rowGroupMeta =>
      val orderedBlockMetaData = rowGroupMeta.asInstanceOf[OrderedBlockMetaData]
      val rows = buildBatchColumnFromCache(orderedBlockMetaData, conf, requiredColumnIds)
      val iter = rows.toIterator
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        iter, requiredColumnIds.foreach(release))
    }
  }

  private def buildIndexedIterator(
      conf: Configuration,
      requiredColumnIds: Array[Int],
      rowIds: Array[Int]): Iterator[InternalRow] = {
    val footer = meta.footer.toParquetMetadata(rowIds)
    footer.getBlocks.asScala.iterator.flatMap { rowGroupMeta =>
      val indexedBlockMetaData = rowGroupMeta.asInstanceOf[IndexedBlockMetaData]
      val rows = buildBatchColumnFromCache(indexedBlockMetaData, conf, requiredColumnIds)
      val iter = indexedBlockMetaData.getNeedRowIds.iterator.
        asScala.map(rowId => rows.moveToRow(rowId))
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        iter, requiredColumnIds.foreach(release))
    }
  }

  private def buildBatchColumnFromCache(
      blockMetaData: OrderedBlockMetaData,
      conf: Configuration,
      requiredColumnIds: Array[Int]): BatchColumn = {
    val rows = new BatchColumn()
    val groupId = blockMetaData.getRowGroupId
    val fiberCacheGroup = requiredColumnIds.map { id =>
      val fiberCache = FiberCacheManager.get(DataFiber(this, id, groupId), conf)
      update(id, fiberCache)
      fiberCache
    }
    val rowCount = blockMetaData.getRowCount.toInt
    val columns = fiberCacheGroup.zip(requiredColumnIds).map { case (fiberCache, id) =>
      new ColumnValues(rowCount, schema(id).dataType, fiberCache)
    }
    rows.reset(rowCount, columns)
    rows
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
