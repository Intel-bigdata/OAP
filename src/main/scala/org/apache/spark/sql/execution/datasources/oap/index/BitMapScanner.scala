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

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.roaringbitmap.buffer.MutableRoaringBitmap
import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.unsafe.Platform

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

  private var bmUniqueKeyListFiber: BitmapFiber = _
  private var bmUniqueKeyListCache: CacheResult = _

  private var bmOffsetListFiber: BitmapFiber = _
  private var bmOffsetListCache: CacheResult = _

  private var bmEntryListFiber: BitmapFiber = _
  private var bmEntryListCache: CacheResult = _

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
    val bmFooterBuffer = new Array[Byte](BITMAP_FOOTER_SIZE)
    fin.readFully(bmFooterOffset, bmFooterBuffer, 0, BITMAP_FOOTER_SIZE)
    bmFooterBuffer
  }

  private def readBmFooterFromCache(cr: CacheResult): Unit = {
    val (baseObj, baseOffset): (AnyRef, Long) = cr.buffer.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (cr.buffer.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    bmUniqueKeyListTotalSize = Platform.getInt(baseObj, baseOffset)
    bmUniqueKeyListCount = Platform.getInt(baseObj, baseOffset + 4)
    bmEntryListTotalSize = Platform.getInt(baseObj, baseOffset + 8)
    bmOffsetListTotalSize = Platform.getInt(baseObj, baseOffset + 12)
  }

  private def loadBmKeyList(fin: FSDataInputStream): Array[Byte] = {
    val bmUniqueKeyListBuffer = new Array[Byte](bmUniqueKeyListTotalSize)
    // TODO: seems not supported yet on my local dev machine(hadoop is 2.7.3).
    // fin.setReadahead(bmUniqueKeyListTotalSize)
    fin.readFully(bmUniqueKeyListOffset, bmUniqueKeyListBuffer, 0, bmUniqueKeyListTotalSize)
    bmUniqueKeyListBuffer
  }

  private def readBmUniqueKeyListFromCache(cr: CacheResult): mutable.ListBuffer[InternalRow] = {
    val (baseObj, baseOffset): (AnyRef, Long) = cr.buffer.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (cr.buffer.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    val uniqueKeyList = new mutable.ListBuffer[InternalRow]()
    var curOffset = baseOffset
    for(idx <- 0 until bmUniqueKeyListCount) {
      val (value, length) =
        IndexUtils.readBasedOnDataType(baseObj, curOffset, keySchema.fields(0).dataType)
      curOffset += length.toInt
      val row = InternalRow.apply(value)
      uniqueKeyList.append(row)
    }
    assert(uniqueKeyList.size == bmUniqueKeyListCount)
    uniqueKeyList
  }

  private def loadBmEntryList(fin: FSDataInputStream): Array[Byte] = {
    val bmEntryListBuffer = new Array[Byte](bmEntryListTotalSize)
    fin.readFully(bmEntryListOffset, bmEntryListBuffer, 0, bmEntryListTotalSize)
    bmEntryListBuffer
  }

  private def loadBmOffsetList(fin: FSDataInputStream): Array[Byte] = {
    val bmOffsetListBuffer = new Array[Byte](bmOffsetListTotalSize)
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

  private def getStartIdxOffset(baseObj: AnyRef, baseOffset: Long, startIdx: Int): Int = {
    val idxOffset = baseOffset + startIdx * 4
    val startIdxOffset = Platform.getInt(baseObj, idxOffset)
    startIdxOffset
  }

  private def getEndIdxOffset(baseObj: AnyRef, baseOffset: Long, endIdx: Int): Int = {
    val idxOffset = baseOffset + endIdx * 4
    val endIdxOffset = Platform.getInt(baseObj, idxOffset)
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

  /**
   * To get the bitmaps, we have to deserialize the data in off-heap memory
   * TODO: Find a way to use bitmap in off-heap
   */
  private def getDesiredBitmaps(byteArray: Array[Byte], position: Int,
      startIdx: Int, endIdx: Int): mutable.ListBuffer[ImmutableRoaringBitmap] = {
    val partialBitmapList = new mutable.ListBuffer[ImmutableRoaringBitmap]()
    val rawBb = ByteBuffer.wrap(byteArray)
    var curPosition = position
    rawBb.position(curPosition)
    for(idx <- startIdx until endIdx) {
      // Below is directly constructed from byte buffer rather than deserializing into java object.
      val bmEntry = new ImmutableRoaringBitmap(rawBb)
      partialBitmapList.append(bmEntry)
      curPosition += bmEntry.serializedSizeInBytes
      rawBb.position(curPosition)
    }
    partialBitmapList
  }

  private def getDesiredBitmapList: List[ImmutableRoaringBitmap] = {
    val keyList = readBmUniqueKeyListFromCache(bmUniqueKeyListCache)
    val (baseObj, baseOffset): (AnyRef, Long) = bmOffsetListCache.buffer.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (bmOffsetListCache.buffer.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    val bitmapList = intervalArray.toList.flatMap(range => {
      val (startIdx, endIdx) = getBitmapIdx(keyList.toIndexedSeq, range)
      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Array.empty[ImmutableRoaringBitmap]
      } else {
        val startIdxOffset = getStartIdxOffset(baseObj, baseOffset, startIdx)
        val curPosition = startIdxOffset - bmEntryListOffset
        getDesiredBitmaps(bmEntryListCache.buffer.toArray, curPosition, startIdx, endIdx + 1)
      }
    })
    bitmapList
  }

  private def initDesiredRowIdIterator(): Unit = {
    val bitmapList = getDesiredBitmapList
    if (bitmapList.nonEmpty) {
      if (limitScanEnabled()) {
        // Get N items from each index.
        bmRowIdIterator = bitmapList.flatMap(bm =>
          bm.iterator.asScala.take(getLimitScanNum)).iterator
      } else {
        val totalBm = new MutableRoaringBitmap()
        bitmapList.foreach(bm => totalBm.or(bm))
        bmRowIdIterator = totalBm.iterator.asScala
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
    assert(keySchema.fields.length == 1)
    this.ordering = GenerateOrdering.create(keySchema)
    val idxPath = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)

    cacheBitmapAllSegments(idxPath, conf)
    initDesiredRowIdIterator()

    this
  }

  override def toString: String = "BitMapScanner"
}
