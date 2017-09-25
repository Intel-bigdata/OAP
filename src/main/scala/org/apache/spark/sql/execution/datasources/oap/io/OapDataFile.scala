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

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.Dictionary
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.{PlainBinaryDictionary, PlainIntegerDictionary}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.{BatchColumn, ColumnValues}
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.types._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}
import org.apache.spark.util.CompletionIterator


private[oap] case class OapDataFile(path: String, schema: StructType,
                                    configuration: Configuration) extends DataFile {

  private val dictionaries = new Array[Dictionary](schema.length)
  private val codecFactory = new CodecFactory(configuration)
  private val meta: OapDataFileHandle = DataFileHandleCacheManager(this)

  def getDictionary(fiberId: Int, conf: Configuration): Dictionary = {
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
        org.apache.parquet.column.Encoding.PLAIN_DICTIONARY)
      schema(fiberId).dataType match {
        case StringType | BinaryType => new PlainBinaryDictionary(dictionaryPage)
        case IntegerType => new PlainIntegerDictionary(dictionaryPage)
        case other => sys.error(s"not support data type: $other")
      }
    } else dictionaries(fiberId)
  }

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): ChunkedByteBuffer = {
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
    is.synchronized {
      is.seek(fiberStart)
      is.readFully(bytes)
    }

    val dataType = schema(fiberId).dataType
    val dictionary = getDictionary(fiberId, conf)
    val fiberParser =
      if (dictionary != null) {
        DictionaryBasedDataFiberParser(encoding, meta, dictionary, dataType)
      } else {
        DataFiberParser(encoding, meta, dataType)
      }

    val rowCount =
      if (groupId == meta.groupCount - 1) meta.rowCountInLastGroup
      else meta.rowCountInEachGroup

    putToFiberCache(fiberParser.parse(decompressor.decompress(bytes, uncompressedLen), rowCount))
  }

  private def putToFiberCache(buf: Array[Byte]): ChunkedByteBuffer = {
    // TODO: make it configurable
    // TODO: [linhong] For now, ChunkedByteBuffer only contains one ByteBuffer
    val cbbos = new ChunkedByteBufferOutputStream(buf.length, ByteBuffer.allocate)
    cbbos.write(buf)
    cbbos.close()
    cbbos.toChunkedByteBuffer
  }

  def closeRowGroup(fiber: Fiber, cacheResult: CacheResult): Unit = {
    if (cacheResult.cached) FiberCacheManager.releaseLock(fiber)
    else cacheResult.buffer.dispose()
  }

  // full file scan
  // TODO: [linhong] two iterator functions are similar. Can we merge them?
  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow] = {
    val row = new BatchColumn()
    val iterator =
      (0 until meta.groupCount).iterator.flatMap { groupId =>
        val cacheResults = requiredIds.map(id =>
          FiberCacheManager.getOrElseUpdate(DataFiber(this, id, groupId), conf))

        val columns = cacheResults.zip(requiredIds).map { case (cacheResult, id) =>
          new ColumnValues(meta.rowCountInEachGroup, schema(id).dataType, cacheResult.buffer)
        }

        val iterator = if (groupId < meta.groupCount - 1) {
          // not the last row group
          row.reset(meta.rowCountInEachGroup, columns).toIterator
        } else {
          row.reset(meta.rowCountInLastGroup, columns).toIterator
        }
        CompletionIterator[InternalRow, Iterator[InternalRow]](iterator,
          cacheResults.zip(requiredIds).foreach {
            case (cacheResult, id) => closeRowGroup(DataFiber(this, id, groupId), cacheResult)
          }
        )
      }
    CompletionIterator[InternalRow, Iterator[InternalRow]](iterator, close())
  }

  // scan by given row ids, and we assume the rowIds are sorted
  def iterator(conf: Configuration, requiredIds: Array[Int], rowIds: Array[Long])
  : Iterator[InternalRow] = {
    val row = new BatchColumn()
    val groupIds = rowIds.groupBy(rowId => (rowId / meta.rowCountInEachGroup).toInt)
    val iterator =
      groupIds.iterator.flatMap {
        case (groupId, subRowIds) =>
          val cacheResults = requiredIds.map(id =>
            FiberCacheManager.getOrElseUpdate(DataFiber(this, id, groupId), conf))

          val columns = cacheResults.zip(requiredIds).map { case (cacheResult, id) =>
            new ColumnValues(meta.rowCountInEachGroup, schema(id).dataType, cacheResult.buffer)
          }

          if (groupId < meta.groupCount - 1) {
            // not the last row group
            row.reset(meta.rowCountInEachGroup, columns)
          } else {
            row.reset(meta.rowCountInLastGroup, columns)
          }

          val iterator =
            subRowIds.iterator.map(rowId => row.moveToRow((rowId % meta.rowCountInEachGroup).toInt))

          CompletionIterator[InternalRow, Iterator[InternalRow]](iterator,
            cacheResults.zip(requiredIds).foreach {
              case (cacheResult, id) => closeRowGroup(DataFiber(this, id, groupId), cacheResult)
            }
          )
      }
    CompletionIterator[InternalRow, Iterator[InternalRow]](iterator, close())
  }

  def close(): Unit = {
    // We don't close DataFileHandle in order to re-use it from cache.
    codecFactory.release()
  }

  override def createDataFileHandle(): OapDataFileHandle = {
    val p = new Path(StringUtils.unEscapeString(path))

    val fs = p.getFileSystem(configuration)

    new OapDataFileHandle().read(fs.open(p), fs.getFileStatus(p).getLen)
  }
}
