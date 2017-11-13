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
package org.apache.spark.sql.execution.datasources.oap.index

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.immutable
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.roaringbitmap.buffer.MutableRoaringBitmap

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer

private[oap] case class BitMapScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = true

  private val BITMAP_FOOTER_SIZE = 5 * 8

  private var bmUniqueKeyListTotalSize: Int = _
  private var bmUniqueKeyListCount: Int = _
  private var bmEntryListTotalSize: Int = _
  private var bmOffsetListTotalSize: Int = _

  private var bmUniqueKeyListOffset: Int = _
  private var bmEntryListOffset: Int = _
  private var bmOffsetListOffset: Int = _
  private var bmFooterOffset: Int = _

  private var bmFooterFiber: BitmapFiber = _
  private var bmFooterCache: CacheResult = _
  private var bmFooterBuffer: Array[Byte] = _

  private var bmUniqueKeyListFiber: BitmapFiber = _
  private var bmUniqueKeyListCache: CacheResult = _
  private var bmUniqueKeyListBuffer: Array[Byte] = _

  private var bmOffsetListFiber: BitmapFiber = _
  private var bmOffsetListCache: CacheResult = _
  private var bmOffsetListBuffer: Array[Byte] = _

  private var bmEntryListFiber: BitmapFiber = _
  private var bmEntryListCache: CacheResult = _
  private var bmEntryListBuffer: Array[Byte] = _

  @transient private var bmRowIdIterator: Iterator[Integer] = _
  private var empty: Boolean = _

  override def hasNext: Boolean = {
    if (!empty && bmRowIdIterator.hasNext) {
      true
    } else {
      if (bmFooterFiber != null) {
        if (bmFooterCache.cached) FiberCacheManager.releaseLock(bmFooterFiber)
        else bmFooterCache.buffer.dispose()
      }

      if (bmUniqueKeyListFiber != null) {
        if (bmUniqueKeyListCache.cached) FiberCacheManager.releaseLock(bmUniqueKeyListFiber)
        else bmUniqueKeyListCache.buffer.dispose()
      }

      if (bmOffsetListFiber != null) {
        if (bmOffsetListCache.cached) FiberCacheManager.releaseLock(bmOffsetListFiber)
        else bmOffsetListCache.buffer.dispose()
      }

      if (bmEntryListFiber != null) {
        if (bmEntryListCache.cached) FiberCacheManager.releaseLock(bmEntryListFiber)
        else bmEntryListCache.buffer.dispose()
      }
      false
    }
  }

  override def next(): Long = bmRowIdIterator.next().toLong

  private def loadBmFooter(fin: FSDataInputStream): Array[Byte] = {
    bmFooterBuffer = new Array[Byte](BITMAP_FOOTER_SIZE)
    fin.read(bmFooterOffset, bmFooterBuffer, 0, BITMAP_FOOTER_SIZE)
    bmFooterBuffer
  }

  private def readBmFooterFromCache(cr: CacheResult): Unit = {
    // In most cases, below cached is true.
    val baseBuffer = if (cr.cached) cr.buffer.toArray else bmFooterBuffer
    bmUniqueKeyListTotalSize = Platform.getInt(baseBuffer, Platform.BYTE_ARRAY_OFFSET)
    bmUniqueKeyListCount = Platform.getInt(baseBuffer, Platform.BYTE_ARRAY_OFFSET + 4)
    bmEntryListTotalSize = Platform.getInt(baseBuffer, Platform.BYTE_ARRAY_OFFSET + 8)
    bmOffsetListTotalSize = Platform.getInt(baseBuffer, Platform.BYTE_ARRAY_OFFSET + 12)
    // bmFooterBuffer is not used any more.
    bmFooterBuffer = null
  }

  private def loadBmKeyList(fin: FSDataInputStream): Array[Byte] = {
    bmUniqueKeyListBuffer = new Array[Byte](bmUniqueKeyListTotalSize)
    // TODO: seems not supported yet on my local dev machine(hadoop is 2.7.3).
    // fin.setReadahead(bmUniqueKeyListTotalSize)
    fin.read(bmUniqueKeyListOffset, bmUniqueKeyListBuffer, 0, bmUniqueKeyListTotalSize)
    bmUniqueKeyListBuffer
  }

  private def readBmUniqueKeyListFromCache(cr: CacheResult): mutable.ListBuffer[InternalRow] = {
    // In most cases, below cached is true.
    val baseBuffer = if (cr.cached) cr.buffer.toArray else bmUniqueKeyListBuffer
    val uniqueKeyList = new mutable.ListBuffer[InternalRow]()
    val (baseObj, baseOffset) = (baseBuffer, Platform.BYTE_ARRAY_OFFSET)
    var curOffset = baseOffset
    (0 until bmUniqueKeyListCount).map(idx => {
      val (value, length) =
        IndexUtils.readBasedOnDataType(baseObj, curOffset, keySchema.fields(0).dataType)
      curOffset += length.toInt
      val row = InternalRow.apply(value)
      uniqueKeyList.append(row)
    })
    assert(uniqueKeyList.size == bmUniqueKeyListCount)
    // bmUniqueKeyListBuffer is not used any more.
    bmUniqueKeyListBuffer = null
    uniqueKeyList
  }

  private def loadBmEntryList(fin: FSDataInputStream): Array[Byte] = {
    bmEntryListBuffer = new Array[Byte](bmEntryListTotalSize)
    fin.read(bmEntryListOffset, bmEntryListBuffer, 0, bmEntryListTotalSize)
    bmEntryListBuffer
  }

  private def loadBmOffsetList(fin: FSDataInputStream): Array[Byte] = {
    bmOffsetListBuffer = new Array[Byte](bmOffsetListTotalSize)
    fin.read(bmOffsetListOffset, bmOffsetListBuffer, 0, bmOffsetListTotalSize)
    bmOffsetListBuffer
  }

  private def cacheBitmapAllSegments(idxPath: Path, conf: Configuration): Unit = {
    val fs = idxPath.getFileSystem(conf)
    val fin = fs.open(idxPath)
    val idxFileSize = fs.getFileStatus(idxPath).getLen.toInt
    bmFooterOffset = idxFileSize - BITMAP_FOOTER_SIZE
    // Cache the segments after first loading from file.
    bmFooterFiber = BitmapFiber(() => loadBmFooter(fin), idxPath.toString, 6, 0)
    bmFooterCache = FiberCacheManager.getOrElseUpdate(bmFooterFiber, conf)
    readBmFooterFromCache(bmFooterCache)

    // Get the offset for the different segments in bitmap index file.
    bmUniqueKeyListOffset = IndexFile.indexFileHeaderLength
    bmEntryListOffset = bmUniqueKeyListOffset + bmUniqueKeyListTotalSize
    bmOffsetListOffset = bmEntryListOffset + bmEntryListTotalSize

    bmUniqueKeyListFiber = BitmapFiber(() => loadBmKeyList(fin), idxPath.toString, 2, 0)
    bmUniqueKeyListCache = FiberCacheManager.getOrElseUpdate(bmUniqueKeyListFiber, conf)

    bmEntryListFiber = BitmapFiber(() => loadBmEntryList(fin), idxPath.toString, 3, 0)
    bmEntryListCache = FiberCacheManager.getOrElseUpdate(bmEntryListFiber, conf)

    bmOffsetListFiber = BitmapFiber(() => loadBmOffsetList(fin), idxPath.toString, 4, 0)
    bmOffsetListCache = FiberCacheManager.getOrElseUpdate(bmOffsetListFiber, conf)
    fin.close()
  }

  private def getStartIdxOffset(offsetListBuffer: Array[Byte], startIdx: Int): Int = {
    val idxOffset = Platform.BYTE_ARRAY_OFFSET + startIdx * 4
    val startIdxOffset = Platform.getInt(offsetListBuffer, idxOffset)
    startIdxOffset
  }

  private def getEndIdxOffset(offsetListBuffer: Array[Byte], endIdx: Int): Int = {
    val idxOffset = Platform.BYTE_ARRAY_OFFSET + endIdx * 4
    val endIdxOffset = Platform.getInt(offsetListBuffer, idxOffset)
    endIdxOffset
  }

  private def getBitmapIdx(keySeq: immutable.IndexedSeq[InternalRow],
      range: RangeInterval): (Int, Int) = {
    val startIdx = if (range.start == IndexScanner.DUMMY_KEY_START) {
      // diff from which startIdx not found, so here startIdx = -2
      -2
    } else {
      // find first key which >= start key, can't find return -1
      if (range.startInclude) {
        keySeq.indexWhere(ordering.compare(range.start, _) <= 0)
      } else {
        keySeq.indexWhere(ordering.compare(range.start, _) < 0)
      }
    }
    val endIdx = if (range.end == IndexScanner.DUMMY_KEY_END) {
      keySeq.size
    } else {
      // find last key which <= end key, can't find return -1
      if (range.endInclude) {
        keySeq.lastIndexWhere(ordering.compare(_, range.end) <= 0)
      } else {
        keySeq.lastIndexWhere(ordering.compare(_, range.end) < 0)
      }
    }
    (startIdx, endIdx)
  }

  private def getDesiredBitmaps(byteArray: Array[Byte], position: Int,
      startIdx: Int, endIdx: Int): mutable.ListBuffer[ImmutableRoaringBitmap] = {
    val partialBitmapList = new mutable.ListBuffer[ImmutableRoaringBitmap]()
    val rawBb = ByteBuffer.wrap(byteArray)
    var curPosition = position
    rawBb.position(curPosition)
    (startIdx until endIdx).map( idx => {
      // Below is directly constructed from byte buffer rather than deserializing into java object.
      val bmEntry = new ImmutableRoaringBitmap(rawBb)
      partialBitmapList.append(bmEntry)
      curPosition += bmEntry.serializedSizeInBytes
      rawBb.position(curPosition)
    })
    partialBitmapList
  }

  private def getDesiredBitmapList(): immutable.List[ImmutableRoaringBitmap] = {
    val keyList = readBmUniqueKeyListFromCache(bmUniqueKeyListCache)
    val entryListBuffer =
      // In most cases, below cached is true.
      if (bmEntryListCache.cached) bmEntryListCache.buffer.toArray
      else bmEntryListBuffer
    val offsetListBuffer =
      // In most cases, below cached is true.
      if (bmOffsetListCache.cached) bmOffsetListCache.buffer.toArray
      else bmOffsetListBuffer
    val bitmapList = intervalArray.toList.flatMap(range => {
      val (startIdx, endIdx) = getBitmapIdx(keyList.toIndexedSeq, range)
      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Array.empty[ImmutableRoaringBitmap]
      } else {
        val startIdxOffset = getStartIdxOffset(offsetListBuffer, startIdx)
        val endIdxOffset = getEndIdxOffset(offsetListBuffer, endIdx + 1)
        val curPostion = startIdxOffset - bmEntryListOffset
        getDesiredBitmaps(entryListBuffer, curPostion, startIdx, (endIdx + 1))
      }
    })
    // They are not used any more.
    bmEntryListBuffer = null
    bmOffsetListBuffer = null
    bitmapList
  }

  private def getDesiredRowIdIterator(): Unit = {
    val bitmapList = getDesiredBitmapList()
    if (bitmapList.nonEmpty) {
      if (limitScanEnabled()) {
        // Get N items from each index.
        bmRowIdIterator = bitmapList.flatMap(bm =>
          bm.iterator.asScala.take(getLimitScanNum)).iterator
      } else {
        var totalBm = new MutableRoaringBitmap()
        bitmapList.foreach(bm => totalBm.or(bm))
        bmRowIdIterator = totalBm.iterator.asScala.toIterator
      }
      empty = false
    } else {
      empty = true
    }
  }

  // TODO: If the index file is not changed, bypass the repetitive initialization for queries.
  override def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // Currently OAP index type supports the column with one single field.
    assert(keySchema.fields.size == 1)
    this.ordering = GenerateOrdering.create(keySchema)
    val idxPath = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)

    cacheBitmapAllSegments(idxPath, conf)
    getDesiredRowIdIterator()

    this
  }

  override def toString: String = "BitMapScanner"
}
