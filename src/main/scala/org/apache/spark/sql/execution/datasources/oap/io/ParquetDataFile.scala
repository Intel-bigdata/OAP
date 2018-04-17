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
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.api.RecordReader

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{MemoryManager, _}
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportWrapper
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

private[oap] case class ParquetDataFile(
    path: String,
    schema: StructType,
    configuration: Configuration) extends DataFile {

  private var context: Option[VectorizedContext] = None
  private lazy val meta: ParquetDataFileHandle = DataFileHandleCacheManager(this)
  private val file = new Path(StringUtils.unEscapeString(path))
  private val parquetDataCacheEnable =
    configuration.getBoolean(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key,
      OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.defaultValue.get)

  def getFiberData(groupId: Int, fiberId: Int): FiberCache = {
    var reader: SingleGroupOapRecordReader = null
    try {
      val conf = new Configuration(configuration)
      val rowGroupRowCount = meta.footer.getBlocks.get(groupId).getRowCount.toInt
      // read a single column for each group.
      // comments: Parquet vectorized read can get multi-columns every time. However,
      // the minimum unit of cache is one column of one group.
      val requiredId = new Array[Int](1)
      requiredId(0) = fiberId
      addRequestSchemaToConf(conf, requiredId)
      reader = new SingleGroupOapRecordReader(file, conf, meta.footer, groupId, rowGroupRowCount)
      reader.initialize()
      reader.initBatch()
      reader.enableReturningBatches()
      if (reader.nextBatch()) {
        val data = reader.getCurrentValue.asInstanceOf[ColumnarBatch].column(0).dumpBytes
        MemoryManager.toDataFiberCache(data)
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

  private def getGroupIdForRowIds(rowIds: Array[Int]): Map[Int, Array[Int]] = {
    val totalCount = rowIds.length
    val groupIdToRowIds = ArrayBuffer[(Int, Array[Int])]()
    var nextRowGroupStartRowId = 0
    var index = 0
    var flag = false
    var blockId = 0
    meta.footer.getBlocks.asScala.foreach(block => {
      val currentRowGroupStartRowId = nextRowGroupStartRowId
      nextRowGroupStartRowId += block.getRowCount.toInt
      flag = true
      val rowIdArray = new ArrayBuffer[Int]()
      while (flag && index < totalCount) {
        val globalRowId = rowIds(index)
        if(globalRowId < nextRowGroupStartRowId) {
          rowIdArray.append(globalRowId - currentRowGroupStartRowId)
          index += 1
        } else {
          flag = false
        }
      }
      if (rowIdArray.nonEmpty) {
        groupIdToRowIds.append((blockId, rowIdArray.toArray))
      }
      blockId += 1
    })
    groupIdToRowIds.toMap
  }

  private[oap] class CacheIterator[T](columnarBatch: ColumnarBatch) extends Iterator[T]
    with Closeable {
    val inner = columnarBatch.rowIterator()
    override def hasNext: Boolean = inner.hasNext
    override def next(): T = inner.next().asInstanceOf[T]
    override def close(): Unit = {}
    def moveToRow(idx: Int): T = columnarBatch.moveToRow(idx).asInstanceOf[T]
  }

  private def buildIterator(
       conf: Configuration,
       requiredColumnIds: Array[Int],
       rowIds: Option[Array[Int]]): OapIterator[InternalRow] = {
    var requestSchema = new StructType
    for (index <- requiredColumnIds) {
      requestSchema = requestSchema.add(schema(index))
    }

    val groupIdToRowIds = rowIds.map(optionRowIds => getGroupIdForRowIds(optionRowIds))
    val groupIds = groupIdToRowIds.map(_.keys).getOrElse(0 until meta.footer.getBlocks.size())
    var fiberCacheGroup: Array[WrappedFiberCache] = null

    val iterator = groupIds.iterator.flatMap { groupId =>
      fiberCacheGroup = requiredColumnIds.map { id =>
        WrappedFiberCache(FiberCacheManager.get(DataFiber(this, id, groupId), conf))
      }
      val rowCount = meta.footer.getBlocks.get(groupId).getRowCount.toInt
      val newRows = ColumnarBatch.allocate(requestSchema, MemoryMode.OFF_HEAP, rowCount)
      newRows.setNumRows(rowCount)
      fiberCacheGroup.zipWithIndex.map { case (fiberCache, id) =>
        newRows.column(id).loadBytes(fiberCache.fc.getBaseOffset)
      }

      val cacheIter = new CacheIterator[InternalRow](newRows)
      val iter = groupIdToRowIds match {
        case Some(map) =>
          map(groupId).iterator.map(rowId => cacheIter.moveToRow(rowId))
        case None =>
          cacheIter
      }

      CompletionIterator[InternalRow, Iterator[InternalRow]](iter,
        fiberCacheGroup.zip(requiredColumnIds).foreach {
          case (fiberCache, id) => fiberCache.release()
        }
      )
    }
    new OapIterator[InternalRow](iterator) {
      override def close(): Unit = {
        // To ensure if any exception happens, caches are still released after calling close()
        if (fiberCacheGroup != null) fiberCacheGroup.foreach(_.release())
      }
    }
  }

  def iterator(requiredIds: Array[Int]): OapIterator[InternalRow] = {
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
          new DefaultRecordReader[UnsafeRow](new ParquetReadSupportWrapper,
            file, configuration, meta.footer))
    }
  }

  def iterator(
      requiredIds: Array[Int],
      rowIds: Array[Int]): OapIterator[InternalRow] = {
    if (rowIds == null || rowIds.length == 0) {
      new OapIterator(Iterator.empty)
    } else {
      addRequestSchemaToConf(configuration, requiredIds)
      val file = new Path(StringUtils.unEscapeString(path))
      val meta: ParquetDataFileHandle = DataFileHandleCacheManager(this)
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
            new OapRecordReader[UnsafeRow](new ParquetReadSupportWrapper,
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

  override def createDataFileHandle(): ParquetDataFileHandle = {
    new ParquetDataFileHandle().read(configuration, new Path(StringUtils.unEscapeString(path)))
  }

  override def totalRows(): Long = {
    import scala.collection.JavaConverters._
    val meta: ParquetDataFileHandle = DataFileHandleCacheManager(this)
    meta.footer.getBlocks.asScala.foldLeft(0L) {
      (sum, block) => sum + block.getRowCount
    }
  }
}
