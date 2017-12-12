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
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.buffer.BufferFastAggregation
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StaticsAnalysisResult
import org.apache.spark.sql.execution.datasources.oap.utils.NonNullKeyReader
import org.apache.spark.unsafe.Platform

private[oap] case class BitMapScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = true

  // TODO: use hash instead of order compare.
  @transient protected var ordering: Ordering[Key] = _
  @transient
  protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(keySchema)

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
  private var bmFooterCache: FiberCache = _

  private var bmUniqueKeyListFiber: BitmapFiber = _
  private var bmUniqueKeyListCache: FiberCache = _

  private var bmOffsetListFiber: BitmapFiber = _
  private var bmOffsetListCache: FiberCache = _

  private var bmEntryListFiber: BitmapFiber = _
  private var bmEntryListCache: FiberCache = _

  @transient private var bmRowIdIterator: Iterator[Integer] = _
  private var empty: Boolean = _

  override def hasNext: Boolean = {
    if (!empty && bmRowIdIterator.hasNext) {
      true
    } else {
      if (bmFooterFiber != null) {
        // TODO: release bmFooterCache usage number
      }

      if (bmUniqueKeyListFiber != null) {
        // TODO: release bmUniqueKeyListCache usage number
      }

      if (bmOffsetListFiber != null) {
        // TODO: release bmOffsetListCache usage number
      }

      if (bmEntryListFiber != null) {
        // TODO: release bmEntryListCache usage number
      }
      false
    }
  }

  override def next(): Int = bmRowIdIterator.next()

  private def loadBmFooter(fin: FSDataInputStream): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, bmFooterOffset, BITMAP_FOOTER_SIZE)
  }

  override protected def readStatistics(indexPath: Path, conf: Configuration): Double = {
    // TODO implement
    StaticsAnalysisResult.USE_INDEX
  }

  private def readBmFooterFromCache(data: FiberCache): Unit = {
    bmUniqueKeyListTotalSize = data.getInt(0)
    bmUniqueKeyListCount = data.getInt(4)
    bmEntryListTotalSize = data.getInt(8)
    bmOffsetListTotalSize = data.getInt(12)
  }

  private def loadBmKeyList(fin: FSDataInputStream): FiberCache = {
    // TODO: seems not supported yet on my local dev machine(hadoop is 2.7.3).
    // fin.setReadahead(bmUniqueKeyListTotalSize)
    MemoryManager.putToIndexFiberCache(fin, bmUniqueKeyListOffset, bmUniqueKeyListTotalSize)
  }

  private def readBmUniqueKeyListFromCache(data: FiberCache): IndexedSeq[InternalRow] = {
    var curOffset = 0
    (0 until bmUniqueKeyListCount).map( idx => {
      val (value, length) =
        nnkr.readKey(data, curOffset)
      curOffset += length
      value
    })
  }

  private def loadBmEntryList(fin: FSDataInputStream): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, bmEntryListOffset, bmEntryListTotalSize)
  }

  private def loadBmOffsetList(fin: FSDataInputStream): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, bmOffsetListOffset, bmOffsetListTotalSize)
  }

  private def cacheBitmapAllSegments(idxPath: Path, conf: Configuration): Unit = {
    val fs = idxPath.getFileSystem(conf)
    val fin = fs.open(idxPath)
    val idxFileSize = fs.getFileStatus(idxPath).getLen.toInt
    bmFooterOffset = idxFileSize - BITMAP_FOOTER_SIZE
    // Cache the segments after first loading from file.
    bmFooterFiber = BitmapFiber(
      () => loadBmFooter(fin), idxPath.toString, BitmapIndexSectionId.footerSection, 0)
    bmFooterCache = FiberCacheManager.get(bmFooterFiber, conf)
    readBmFooterFromCache(bmFooterCache)

    // Get the offset for the different segments in bitmap index file.
    bmUniqueKeyListOffset = IndexFile.indexFileHeaderLength
    bmEntryListOffset = bmUniqueKeyListOffset + bmUniqueKeyListTotalSize
    bmOffsetListOffset = bmEntryListOffset + bmEntryListTotalSize

    bmUniqueKeyListFiber = BitmapFiber(
        () => loadBmKeyList(fin), idxPath.toString, BitmapIndexSectionId.keyListSection, 0)
    bmUniqueKeyListCache = FiberCacheManager.get(bmUniqueKeyListFiber, conf)

    bmEntryListFiber = BitmapFiber(
      () => loadBmEntryList(fin), idxPath.toString, BitmapIndexSectionId.entryListSection, 0)
    bmEntryListCache = FiberCacheManager.get(bmEntryListFiber, conf)

    bmOffsetListFiber = BitmapFiber(
      () => loadBmOffsetList(fin), idxPath.toString, BitmapIndexSectionId.entryOffsetsSection, 0)
    bmOffsetListCache = FiberCacheManager.get(bmOffsetListFiber, conf)
    fin.close()
  }

  private def getStartIdxOffset(fiberCache: FiberCache, baseOffset: Long, startIdx: Int): Int = {
    val idxOffset = baseOffset + startIdx * 4
    val startIdxOffset = fiberCache.getInt(idxOffset)
    startIdxOffset
  }

  private def getEndIdxOffset(fiberCache: FiberCache, baseOffset: Long, endIdx: Int): Int = {
    val idxOffset = baseOffset + endIdx * 4
    val endIdxOffset = Platform.getInt(FiberCache, idxOffset)
    endIdxOffset
  }

  private def getBitmapIdx(keySeq: IndexedSeq[InternalRow],
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
      startIdx: Int, endIdx: Int): IndexedSeq[ImmutableRoaringBitmap] = {
    val rawBb = ByteBuffer.wrap(byteArray)
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

  private def getDesiredBitmapArray: mutable.ArrayBuffer[ImmutableRoaringBitmap] = {
    val keySeq = readBmUniqueKeyListFromCache(bmUniqueKeyListCache)
    intervalArray.flatMap(range => {
      val (startIdx, endIdx) = getBitmapIdx(keySeq, range)
      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Seq.empty[ImmutableRoaringBitmap]
      } else {
        val startIdxOffset = getStartIdxOffset(bmOffsetListCache, 0L, startIdx)
        val curPosition = startIdxOffset - bmEntryListOffset
        getDesiredBitmaps(bmEntryListCache.toArray, curPosition, startIdx, endIdx + 1)
      }
    })
  }

  private def initDesiredRowIdIterator(): Unit = {
    val bitmapArray = getDesiredBitmapArray
    if (bitmapArray.nonEmpty) {
      if (indexEntryScanIsLimited()) {
        // Get N items from each index.
        bmRowIdIterator = bitmapArray.flatMap(bm =>
          bm.iterator.asScala.take(internalLimit)).iterator
      } else {
        bmRowIdIterator =
          bitmapArray.reduceLeft(BufferFastAggregation.or(_, _)).iterator.asScala
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
