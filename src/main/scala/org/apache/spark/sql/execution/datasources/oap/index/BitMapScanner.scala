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
import org.roaringbitmap.buffer.BufferFastAggregation
import sun.nio.ch.DirectBuffer

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

  // If using BitSet, just use below structure.
  // @transient private var bmRowIdIterator: Iterator[Int] = _
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
    fin.readFully(bmFooterOffset, bmFooterBuffer, 0, BITMAP_FOOTER_SIZE)
    bmFooterBuffer
  }

  private def readBmFooterFromCache(cr: CacheResult): Unit = {
    // In most cases, below cached is true.
    val (baseBuffer, baseOffset): (Object, Long) = if (cr.cached) cr.buffer.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (cr.buffer.toArray, Platform.BYTE_ARRAY_OFFSET) }
      else (bmFooterBuffer, Platform.BYTE_ARRAY_OFFSET)
    bmUniqueKeyListTotalSize = Platform.getInt(baseBuffer, baseOffset)
    bmUniqueKeyListCount = Platform.getInt(baseBuffer, baseOffset + 4)
    bmEntryListTotalSize = Platform.getInt(baseBuffer, baseOffset + 8)
    bmOffsetListTotalSize = Platform.getInt(baseBuffer, baseOffset + 12)
    // bmFooterBuffer is not used any more.
    bmFooterBuffer = null
  }

  private def loadBmKeyList(fin: FSDataInputStream): Array[Byte] = {
    bmUniqueKeyListBuffer = new Array[Byte](bmUniqueKeyListTotalSize)
    // TODO: seems not supported yet on my local dev machine(hadoop is 2.7.3).
    // fin.setReadahead(bmUniqueKeyListTotalSize)
    fin.readFully(bmUniqueKeyListOffset, bmUniqueKeyListBuffer, 0, bmUniqueKeyListTotalSize)
    bmUniqueKeyListBuffer
  }

  private def readBmUniqueKeyListFromCache(cr: CacheResult): mutable.ListBuffer[InternalRow] = {
    // In most cases, below cached is true.
    val (baseBuffer, baseOffset): (Object, Long) = if (cr.cached) cr.buffer.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (cr.buffer.toArray, Platform.BYTE_ARRAY_OFFSET) }
      else (bmUniqueKeyListBuffer, Platform.BYTE_ARRAY_OFFSET)
    val uniqueKeyList = new mutable.ListBuffer[InternalRow]()
    var curOffset = baseOffset
    (0 until bmUniqueKeyListCount).map(idx => {
      val (value, length) =
        IndexUtils.readBasedOnDataType(baseBuffer, curOffset, keySchema.fields(0).dataType)
      curOffset += length
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
    fin.readFully(bmEntryListOffset, bmEntryListBuffer, 0, bmEntryListTotalSize)
    bmEntryListBuffer
  }

  private def loadBmOffsetList(fin: FSDataInputStream): Array[Byte] = {
    bmOffsetListBuffer = new Array[Byte](bmOffsetListTotalSize)
    fin.readFully(bmOffsetListOffset, bmOffsetListBuffer, 0, bmOffsetListTotalSize)
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

  private def getStartIdxOffset(baseBuffer: Object, baseOffset: Long, startIdx: Int): Int = {
    val idxOffset = baseOffset + startIdx * 4
    val startIdxOffset = Platform.getInt(baseBuffer, idxOffset)
    startIdxOffset
  }

  private def getEndIdxOffset(baseBuffer: Object, baseOffset: Long, endIdx: Int): Int = {
    val idxOffset = baseOffset + endIdx * 4
    val endIdxOffset = Platform.getInt(baseBuffer, idxOffset)
    endIdxOffset
  }

  private def getBitmapIdx(keyList: mutable.ListBuffer[InternalRow],
      range: RangeInterval): (Int, Int) = {
    val startIdx = if (range.start == IndexScanner.DUMMY_KEY_START) {
      // diff from which startIdx not found, so here startIdx = -2
      -2
    } else {
      // find first key which >= start key, can't find return -1
      if (range.startInclude) {
        keyList.indexWhere(ordering.compare(range.start, _) <= 0)
      } else {
        keyList.indexWhere(ordering.compare(range.start, _) < 0)
      }
    }
    val endIdx = if (range.end == IndexScanner.DUMMY_KEY_END) {
      keyList.size
    } else {
      // find last key which <= end key, can't find return -1
      if (range.endInclude) {
        keyList.lastIndexWhere(ordering.compare(_, range.end) <= 0)
      } else {
        keyList.lastIndexWhere(ordering.compare(_, range.end) < 0)
      }
    }
    (startIdx, endIdx)
  }

  private def getDesiredBitmapsUsingBitSet(byteArray: Object, baseOffset: Long, position: Int,
      startIdx: Int, endIdx: Int): immutable.IndexedSeq[immutable.BitSet] = {
    var curPosition = baseOffset + position
    (startIdx until endIdx).map( idx => {
      val bmLongArraySize = Platform.getInt(byteArray, curPosition)
      val rawLongArray = new Array[Long](bmLongArraySize)
      curPosition += 4
      (0 until bmLongArraySize).map(idx => {
        rawLongArray(idx) = Platform.getLong(byteArray, curPosition)
        curPosition += 8
      })
      immutable.BitSet.fromBitMask(rawLongArray)
    })
  }

  private def getDesiredBitmaps(byteArray: Object, baseOffset: Long, position: Int,
      startIdx: Int, endIdx: Int): immutable.IndexedSeq[ImmutableRoaringBitmap] = {
    val rawByteArray = (0 until bmEntryListTotalSize).map( idx => {
      Platform.getByte(byteArray, baseOffset + idx)
    }).toArray
    val rawBb = ByteBuffer.wrap(rawByteArray)
    var curPosition = position
    rawBb.position(curPosition)
    (startIdx until endIdx).map( idx => {
      // Below is directly constructed from byte buffer rather than deserializing into java object.
      val bmEntry = new ImmutableRoaringBitmap(rawBb)
      curPosition += bmEntry.serializedSizeInBytes
      rawBb.position(curPosition)
      bmEntry
    })
  }

  private def getDesiredBitmapArrayUsingBitSet(): mutable.ArrayBuffer[immutable.BitSet] = {
    val keyList = readBmUniqueKeyListFromCache(bmUniqueKeyListCache)
    val (entryListBuffer, entryListBaseOffset): (Object, Long) =
      // In most cases, below cached is true.
      if (bmEntryListCache.cached) bmEntryListCache.buffer.chunks.head match {
        case db: DirectBuffer => (null, db.address())
        case _ => (bmEntryListCache.buffer.toArray, Platform.BYTE_ARRAY_OFFSET) }
      else (bmEntryListBuffer, Platform.BYTE_ARRAY_OFFSET)
    val (offsetListBuffer, offsetListBaseOffset): (Object, Long) =
      // In most cases, below cached is true.
      if (bmOffsetListCache.cached) bmOffsetListCache.buffer.chunks.head match {
        case db: DirectBuffer => (null, db.address())
        case _ => (bmOffsetListCache.buffer.toArray, Platform.BYTE_ARRAY_OFFSET) }
      else (bmOffsetListBuffer, Platform.BYTE_ARRAY_OFFSET)
    val bitmapArray = intervalArray.flatMap(range => {
      val (startIdx, endIdx) = getBitmapIdx(keyList, range)
      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Seq.empty[immutable.BitSet]
      } else {
        val startIdxOffset = getStartIdxOffset(offsetListBuffer, offsetListBaseOffset, startIdx)
        val endIdxOffset = getEndIdxOffset(offsetListBuffer, offsetListBaseOffset, endIdx + 1)
        val curPostion = startIdxOffset - bmEntryListOffset
        getDesiredBitmapsUsingBitSet(entryListBuffer, entryListBaseOffset, curPostion,
          startIdx, (endIdx + 1))
      }
    })
    // They are not used any more.
    bmEntryListBuffer = null
    bmOffsetListBuffer = null
    bitmapArray
  }

  private def getDesiredBitmapArray(): mutable.ArrayBuffer[ImmutableRoaringBitmap] = {
    val keyList = readBmUniqueKeyListFromCache(bmUniqueKeyListCache)
    val (entryListBuffer, entryListBaseOffset): (Object, Long) =
      // In most cases, below cached is true.
      if (bmEntryListCache.cached) bmEntryListCache.buffer.chunks.head match {
        case db: DirectBuffer => (null, db.address())
        case _ => (bmEntryListCache.buffer.toArray, Platform.BYTE_ARRAY_OFFSET) }
      else (bmEntryListBuffer, Platform.BYTE_ARRAY_OFFSET)
    val (offsetListBuffer, offsetListBaseOffset): (Object, Long) =
      // In most cases, below cached is true.
      if (bmOffsetListCache.cached) bmOffsetListCache.buffer.chunks.head match {
        case db: DirectBuffer => (null, db.address())
        case _ => (bmOffsetListCache.buffer.toArray, Platform.BYTE_ARRAY_OFFSET) }
      else (bmOffsetListBuffer, Platform.BYTE_ARRAY_OFFSET)
    val bitmapArray = intervalArray.flatMap(range => {
      val (startIdx, endIdx) = getBitmapIdx(keyList, range)
      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Seq.empty[ImmutableRoaringBitmap]
      } else {
        val startIdxOffset = getStartIdxOffset(offsetListBuffer, offsetListBaseOffset, startIdx)
        val endIdxOffset = getEndIdxOffset(offsetListBuffer, offsetListBaseOffset, endIdx + 1)
        val curPostion = startIdxOffset - bmEntryListOffset
        getDesiredBitmaps(entryListBuffer, entryListBaseOffset, curPostion, startIdx, (endIdx + 1))
      }
    })
    // They are not used any more.
    bmEntryListBuffer = null
    bmOffsetListBuffer = null
    bitmapArray
  }

/*
  private def getDesiredRowIdIteratorUsingBitSet(): Unit = {
    val bitmapArray = getDesiredBitmapArrayUsingBitSet()
    if (bitmapArray.nonEmpty) {
      if (limitScanEnabled()) {
        // Get N items from each index.
        bmRowIdIterator = bitmapArray.flatMap(bm =>
          bm.iterator.take(getLimitScanNum)).iterator
      } else {
        bmRowIdIterator = bitmapArray.reduceLeft(_ | _).iterator
      }
      empty = false
    } else {
      empty = true
    }
  }
*/

  private def getDesiredRowIdIterator(): Unit = {
    val bitmapArray = getDesiredBitmapArray()
    if (bitmapArray.nonEmpty) {
      if (limitScanEnabled()) {
        // Get N items from each index.
        bmRowIdIterator = bitmapArray.flatMap(bm =>
          bm.iterator.asScala.take(getLimitScanNum)).iterator
      } else {
        bmRowIdIterator =
          bitmapArray.reduceLeft(BufferFastAggregation.or(_, _)).iterator.asScala.toIterator
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
    // If using BitSet, just replace with getDesiredRowIdIteratorUsingBitSet.
    getDesiredRowIdIterator()

    this
  }

  override def toString: String = "BitMapScanner"
}
