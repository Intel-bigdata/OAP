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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.{BitmapFiberId, FiberCache, FiberId}
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileReaderImpl
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.{StatisticsManager, StatsAnalysisResult}
import org.apache.spark.sql.execution.datasources.oap.utils.NonNullKeyReader
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types.StructType

private[oap] case class BitmapReader(
    fileReader: IndexFileReaderImpl,
    intervalArray: ArrayBuffer[RangeInterval],
    keySchema: StructType,
    conf: Configuration) {

  private var _totalRows: Long = 0
  // TODO: use hash instead of order compare.
  @transient protected var ordering: Ordering[Key] =
    GenerateOrdering.create(keySchema)
  @transient
  protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(keySchema)

  protected val fiberCacheManager = OapRuntime.getOrCreate.fiberCacheManager

  protected val BITMAP_FOOTER_SIZE = 6 * 8

  protected var bmUniqueKeyListCount: Int = _

  protected var bmUniqueKeyListCache: FiberCache = _
  protected var bmOffsetListCache: FiberCache = _
  protected var bmFooterCache: FiberCache = _

  protected var bmNullListFiber: FiberId = _
  protected var bmNullEntryOffset: Int = _
  protected var bmNullEntrySize: Int = _

  override def toString: String = "BitmapReader"

  private def getFooterCache(): Unit = {
    val footerOffset = fileReader.getLen.toInt - BITMAP_FOOTER_SIZE
    val footerFiber = BitmapFiberId(
      () => fileReader.readFiberCache(footerOffset, BITMAP_FOOTER_SIZE),
      fileReader.getName, BitmapIndexSectionId.footerSection, 0)
    bmFooterCache = fiberCacheManager.get(footerFiber)
    // Calculate total rows right after footer cache is loaded.
    _totalRows = bmFooterCache.getInt(IndexUtils.INT_SIZE * 7)

  }

  private def checkVersionNum(versionNum: Int): Unit =
    if (IndexFile.VERSION_NUM != versionNum) {
      throw new OapException("Bitmap Index File version is not compatible!")
    }

  protected def getIdxOffset(fiberCache: FiberCache, baseOffset: Long, idx: Int): Int =
    fiberCache.getInt(baseOffset + idx * 4)

  protected def getKeyIdx(keySeq: Seq[InternalRow], range: RangeInterval): (Int, Int) = {
    val keyLength = keySeq.length
    val startIdx = if (range.start == IndexScanner.DUMMY_KEY_START) {
      // If no starting key, assume to start from the first key.
      0
    } else {
     // Find the first index to be > or >= range.start. If no found, return -1.
      val (idx, found) =
         IndexUtils.binarySearch(0, keyLength, keySeq(_), range.start, ordering.compare)
      if (found) {
        if (range.startInclude) idx else idx + 1
      } else if (ordering.compare(keySeq.head, range.start) > 0) {
        0
      } else {
        -1
      }
    }
    // If invalid starting index, just return.
    if (startIdx == -1 || startIdx == keyLength) {
      return (-1, -1)
    }
    // If equal query, no need to find endIdx.
    if (range.start == range.end && range.start != IndexScanner.DUMMY_KEY_START) {
      return (startIdx, startIdx)
    }

    val endIdx = if (range.end == IndexScanner.DUMMY_KEY_END) {
      // If no ending key, assume to end with the last key.
      keyLength - 1
    } else {
      // The range may be invalid. I.e. endIdx may be little than startIdx.
      // So find endIdx from the beginning.
      val (idx, found) =
         IndexUtils.binarySearch(0, keyLength, keySeq(_), range.end, ordering.compare)
      if (found) {
        if (range.endInclude) idx else idx - 1
      } else if (ordering.compare(keySeq.last, range.end) < 0) {
        keyLength - 1
      } else {
        -1
      }
    }
    (startIdx, endIdx)
  }

  protected def readBmUniqueKeyList(data: FiberCache): Seq[InternalRow] = {
    var curOffset = 0
    (0 until bmUniqueKeyListCount).map( idx => {
      val (value, length) =
        nnkr.readKey(data, curOffset)
      curOffset += length
      value
    })
  }

  protected def initDesiredSegments(): Unit = {
    getFooterCache()
    assert(bmFooterCache != null)
    val versionNum = bmFooterCache.getInt(0)
    checkVersionNum(versionNum)
    val uniqueKeyListTotalSize = bmFooterCache.getInt(IndexUtils.INT_SIZE)
    bmUniqueKeyListCount = bmFooterCache.getInt(IndexUtils.INT_SIZE * 2)
    val entryListTotalSize = bmFooterCache.getInt(IndexUtils.INT_SIZE * 3)
    val offsetListTotalSize = bmFooterCache.getInt(IndexUtils.INT_SIZE * 4)
    bmNullEntryOffset = bmFooterCache.getInt(IndexUtils.INT_SIZE * 5)
    bmNullEntrySize = bmFooterCache.getInt(IndexUtils.INT_SIZE * 6)

    // Get the offset for the different segments in bitmap index file.
    val uniqueKeyListOffset = IndexFile.VERSION_LENGTH
    val entryListOffset = uniqueKeyListOffset + uniqueKeyListTotalSize
    val offsetListOffset = entryListOffset + entryListTotalSize + bmNullEntrySize

    val uniqueKeyListFiber = BitmapFiberId(
      () => fileReader.readFiberCache(uniqueKeyListOffset, uniqueKeyListTotalSize),
      fileReader.getName, BitmapIndexSectionId.keyListSection, 0)
    bmUniqueKeyListCache = fiberCacheManager.get(uniqueKeyListFiber)

    val offsetListFiber = BitmapFiberId(
      () => fileReader.readFiberCache(offsetListOffset, offsetListTotalSize),
      fileReader.getName, BitmapIndexSectionId.entryOffsetsSection, 0)
    bmOffsetListCache = fiberCacheManager.get(offsetListFiber)

    bmNullListFiber = BitmapFiberId(
      () => fileReader.readFiberCache(bmNullEntryOffset, bmNullEntrySize),
      fileReader.getName, BitmapIndexSectionId.entryNullSection, 0)
  }

  protected def clearCache(): Unit = {
    if (bmUniqueKeyListCache != null) {
      bmUniqueKeyListCache.release
    }
    if (bmOffsetListCache != null) {
      bmOffsetListCache.release
    }
    if (bmFooterCache != null) {
      bmFooterCache.release
    }
  }

  def totalRows(): Long = _totalRows

  def analyzeStatistics(): StatsAnalysisResult = {
    getFooterCache()
    // The stats offset and size are located in the end of bitmap footer segment.
    // See the comments in BitmapIndexRecordWriter.scala.
    val statsOffset = bmFooterCache.getLong(BITMAP_FOOTER_SIZE - IndexUtils.LONG_SIZE * 2)
    val statsSize = bmFooterCache.getLong(BITMAP_FOOTER_SIZE - IndexUtils.LONG_SIZE)
    val bmStatsContentFiber = BitmapFiberId(
      () => fileReader.readFiberCache(statsOffset.toInt, statsSize.toInt),
      fileReader.getName, BitmapIndexSectionId.statsContentSection, 0)
    val bmStatsContentCache = fiberCacheManager.get(bmStatsContentFiber)
    val stats = StatisticsManager.read(bmStatsContentCache, 0, keySchema)
    val res = StatisticsManager.analyse(stats, intervalArray, conf)
    bmFooterCache.release
    bmStatsContentCache.release
    res
  }
}
