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
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.Dictionary
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.{PlainBinaryDictionary, PlainIntegerDictionary}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[oap] abstract class OapDataFile extends DataFile {
  // Currently this is for a more clear class hierarchy, in the future there may be some common
  // parts OAP data file be put here
}

private[oap] case class OapDataFileV1(
    path: String,
    schema: StructType,
    configuration: Configuration) extends OapDataFile {

  private val dictionaries = new Array[Dictionary](schema.length)
  private val codecFactory = new CodecFactory(configuration)
  private val meta =
    OapRuntime.getOrCreate.dataFileMetaCacheManager.get(this).asInstanceOf[OapDataFileMetaV1]

  private def isSkippedByRowGroup(filters: Seq[Filter] = Nil, rowGroupId: Int): Boolean = {
    filters.exists(filter =>
      isSkippedByStatistics(meta.rowGroupsMeta(rowGroupId).statistics, filter, schema))
  }

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

  def getDictionary(fiberId: Int): Dictionary = {
    val lastGroupMeta = meta.rowGroupsMeta(meta.groupCount - 1)
    val dictDataLens = meta.columnsMeta.map(_.dictionaryDataLength)

    val dictStart = lastGroupMeta.end + dictDataLens.slice(0, fiberId).sum
    val dataLen = dictDataLens(fiberId)
    val dictSize = meta.columnsMeta(fiberId).dictionaryIdSize
    if (dictionaries(fiberId) == null && dataLen != 0) {
      val bytes = new Array[Byte](dataLen)
      val is = meta.fin
      is.synchronized {
        is.seek(dictStart)
        is.readFully(bytes)
      }
      val dictionaryPage = new DictionaryPage(BytesInput.from(bytes), dictSize,
        org.apache.parquet.column.Encoding.PLAIN)
      schema(fiberId).dataType match {
        case StringType | BinaryType => new PlainBinaryDictionary(dictionaryPage)
        case IntegerType => new PlainIntegerDictionary(dictionaryPage)
        case other => sys.error(s"not support data type: $other")
      }
    } else {
      dictionaries(fiberId)
    }
  }

  def cache(groupId: Int, fiberId: Int): FiberCache = {
    val groupMeta = meta.rowGroupsMeta(groupId)
    val decompressor: BytesDecompressor = codecFactory.getDecompressor(meta.codec)

    // get the fiber data start position
    // TODO: update the meta to store the fiber start pos
    var i = 0
    var fiberStart = groupMeta.start
    while (i < fiberId) {
      fiberStart += groupMeta.fiberLens(i)
      i += 1
    }
    val len = groupMeta.fiberLens(fiberId)
    val uncompressedLen = groupMeta.fiberUncompressedLens(fiberId)
    val encoding = meta.columnsMeta(fiberId).encoding

    val bytes = new Array[Byte](len)

    val is = meta.fin
    // TODO: replace by FSDataInputStream.readFully(position, buffer) which is thread safe
    is.synchronized {
      is.seek(fiberStart)
      is.readFully(bytes)
    }

    val dataType = schema(fiberId).dataType
    val dictionary = getDictionary(fiberId)
    val fiberParser =
      if (dictionary != null) {
        DictionaryBasedDataFiberParser(encoding, meta, dictionary, dataType)
      } else {
        DataFiberParser(encoding, meta, dataType)
      }

    val rowCount =
      if (groupId == meta.groupCount - 1) {
        meta.rowCountInLastGroup
      } else {
        meta.rowCountInEachGroup
      }

    // We have to read Array[Byte] from file and decode/decompress it before putToFiberCache
    // TODO: Try to finish this in off-heap memory
    val data = fiberParser.parse(decompressor.decompress(bytes, uncompressedLen), rowCount)
    OapRuntime.getOrCreate.memoryManager.toDataFiberCache(data)
  }

  private def buildIterator(
      conf: Configuration,
      requiredIds: Array[Int],
      rowIds: Option[Array[Int]],
      filters: Seq[Filter]): OapCompletionIterator[InternalRow] = {
    val rows = new BatchColumn()
    val groupIdToRowIds = rowIds.map(_.groupBy(rowId => rowId / meta.rowCountInEachGroup))
    val groupIds = groupIdToRowIds.map(_.keys).getOrElse(0 until meta.groupCount)

    val groupIdsNonSkipped = if (filters.isEmpty) {
      groupIds.iterator
    } else {
      groupIds.iterator.filterNot(isSkippedByRowGroup(filters, _))
    }

    val iterator = groupIdsNonSkipped.flatMap {
      groupId =>
        val fiberCacheGroup = requiredIds.map { id =>
          val fiberCache = OapRuntime.getOrCreate.fiberCacheManager.get(
            DataFiberId(this, id, groupId))
          update(id, fiberCache)
          fiberCache
        }

        val columns = fiberCacheGroup.zip(requiredIds).map { case (fiberCache, id) =>
          new ColumnValues(meta.rowCountInEachGroup, schema(id).dataType, fiberCache)
        }

        val rowCount =
          if (groupId < meta.groupCount - 1) meta.rowCountInEachGroup else meta.rowCountInLastGroup
        rows.reset(rowCount, columns)

        groupIdToRowIds match {
          case Some(map) =>
            map(groupId).iterator.map(rowId => rows.moveToRow(rowId % meta.rowCountInEachGroup))
          case None => rows.toIterator
        }
    }
    new OapCompletionIterator[InternalRow](iterator, inUseFiberCache.indices.foreach(release)) {
      override def close(): Unit = {
        // To ensure if any exception happens, caches are still released after calling close()
        inUseFiberCache.indices.foreach(release)
        OapDataFileV1.this.close()
      }
    }
  }

  // full file scan
  def iterator(requiredIds: Array[Int], filters: Seq[Filter] = Nil)
    : OapCompletionIterator[InternalRow] = {
    buildIterator(configuration, requiredIds, rowIds = None, filters)
  }

  // scan by given row ids, and we assume the rowIds are sorted
  def iteratorWithRowIds(
      requiredIds: Array[Int],
      rowIds: Array[Int],
      filters: Seq[Filter] = Nil): OapCompletionIterator[InternalRow] = {
    buildIterator(configuration, requiredIds, Some(rowIds), filters)
  }

  def close(): Unit = {
    // We don't close DataFileMeta in order to re-use it from cache.
    codecFactory.release()
  }

  override def getDataFileMeta(): OapDataFileMetaV1 = {
    val p = new Path(StringUtils.unEscapeString(path))

    val fs = p.getFileSystem(configuration)

    new OapDataFileMetaV1().read(fs.open(p), fs.getFileStatus(p).getLen)
  }

  def totalRows(): Long = meta.totalRowCount()
}
