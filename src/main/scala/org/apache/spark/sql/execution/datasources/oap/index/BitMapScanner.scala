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

import java.io.DataInput
import java.io.EOFException

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.FastAggregation
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager
import org.apache.spark.sql.execution.datasources.oap.utils.NonNullKeyReader
import org.apache.spark.util.ShutdownHookManager

private[oap] case class BitMapScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = true

  // TODO: use hash instead of order compare.
  @transient protected var ordering: Ordering[Key] = _
  @transient
  protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(keySchema)

  private val BITMAP_FOOTER_SIZE = 4 + 5 * 8

  private var bmUniqueKeyListTotalSize: Int = _
  private var bmUniqueKeyListCount: Int = _

  private var bmFooterFiber: BitmapFiber = _
  private var bmFooterCache: WrappedFiberCache = _

  private var bmUniqueKeyListFiber: BitmapFiber = _
  private var bmUniqueKeyListCache: WrappedFiberCache = _

  private var bmOffsetListFiber: BitmapFiber = _
  private var bmOffsetListCache: WrappedFiberCache = _

  private var bmNullListFiber: BitmapFiber = _
  private var bmNullListCache: WrappedFiberCache = _

  private var bmEntryListFiber: BitmapFiber = _
  private var bmEntryListCache: WrappedFiberCache = _

  private var fin: FSDataInputStream = _

  @transient private var bmRowIdIterator: Iterator[Integer] = _
  private var empty: Boolean = _

  override def hasNext: Boolean = !empty && bmRowIdIterator.hasNext

  override def next(): Int = bmRowIdIterator.next()

  private def loadBmFooter(fin: FSDataInputStream, bmFooterOffset: Int): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, bmFooterOffset, BITMAP_FOOTER_SIZE)
  }

  private def loadBmStatsContent(fin: FSDataInputStream, offset: Long, size: Long): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, offset, size.toInt)
  }

  private def cacheBitmapFooterSegment(idxPath: Path, conf: Configuration): Unit = {
    val fs = idxPath.getFileSystem(conf)
    // Cache the file inputstream rather than opening it each query.
    if (fin == null) {
      fin = fs.open(idxPath)
    }
    val idxFileSize = fs.getFileStatus(idxPath).getLen
    val bmFooterOffset = idxFileSize.toInt - BITMAP_FOOTER_SIZE

    if (bmFooterFiber == null) {
      bmFooterFiber = BitmapFiber(
        () => loadBmFooter(fin, bmFooterOffset),
        idxPath.toString, BitmapIndexSectionId.footerSection, 0)
    }
    if (bmFooterCache == null) {
      bmFooterCache = WrappedFiberCache(FiberCacheManager.get(bmFooterFiber, conf))
    }
  }

  override protected def analyzeStatistics(indexPath: Path, conf: Configuration): Double = {
    var bmStatsContentCache: WrappedFiberCache = null
    try {
      val fs = indexPath.getFileSystem(conf)
      fin = fs.open(indexPath)
      cacheBitmapFooterSegment(indexPath, conf)
      // The stats offset and size are located in the end of bitmap footer segment.
      // See the comments in BitmapIndexRecordWriter.scala.
      val statsOffset = bmFooterCache.fc.getLong(BITMAP_FOOTER_SIZE - IndexUtils.LONG_SIZE * 2)
      val statsSize = bmFooterCache.fc.getLong(BITMAP_FOOTER_SIZE - IndexUtils.LONG_SIZE)
      // We expect stats fiber and cache to reside in memory for the whole lifecycle of
      // BitmapScanner.
      // Thus we will release them lazily together with other bitmap fiber and cache.
      val bmStatsContentFiber = BitmapFiber(
        () => loadBmStatsContent(fin, statsOffset, statsSize),
        indexPath.toString, BitmapIndexSectionId.statsContentSection, 0)
      bmStatsContentCache = WrappedFiberCache(FiberCacheManager.get(bmStatsContentFiber, conf))

      val stats = StatisticsManager.read(bmStatsContentCache.fc, 0, keySchema)
      StatisticsManager.analyse(stats, intervalArray, conf)
    } finally {
      if (bmFooterCache != null) {
        bmFooterCache.release()
        bmFooterCache = null
      }
      if (bmStatsContentCache != null) {
        bmStatsContentCache.release()
      }
      try {
        if (fin != null) {
          fin.close()
        }
      } catch {
        case e: Exception =>
          if (!ShutdownHookManager.inShutdown()) {
            logWarning("Exception in FSDataInputStream.close()", e)
          }
      } finally {
        fin = null
      }
    }
  }

  private def loadBmKeyList(fin: FSDataInputStream, bmUniqueKeyListOffset: Int): FiberCache = {
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

  private def loadBmEntryList(
      fin: FSDataInputStream, bmEntryListOffset: Int, bmEntryListTotalSize: Int): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, bmEntryListOffset, bmEntryListTotalSize)
  }

  private def loadBmOffsetList(
      fin: FSDataInputStream,
      bmOffsetListOffset: Int,
      bmOffsetListTotalSize: Int): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, bmOffsetListOffset, bmOffsetListTotalSize)
  }

  private def loadBmNullList(
      fin: FSDataInputStream, bmNullEntryOffset: Int, bmNullEntrySize: Int): FiberCache = {
    MemoryManager.putToIndexFiberCache(fin, bmNullEntryOffset, bmNullEntrySize)
  }

  private def checkVersionNum(versionNum: Int, fin: FSDataInputStream): Unit = {
    if (IndexFile.VERSION_NUM != versionNum) {
      throw new OapException("Bitmap Index File version is not compatible!")
    }
  }

  private def getIndexVersionNum: Int = {
    assert(bmFooterCache != null)
    bmFooterCache.fc.getInt(0)
  }

  private def cacheBitmapAllSegments(idxPath: Path, conf: Configuration): Unit = {
    val fs = idxPath.getFileSystem(conf)
    try {
      fin = fs.open(idxPath)
      // If the executor index selection is disabled, then directly use index to bypass stats.
      // Thus we need to ensure that bitmap footer is loaded already.
      cacheBitmapFooterSegment(idxPath, conf)
      checkVersionNum(getIndexVersionNum, fin)
      bmUniqueKeyListTotalSize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE)
      bmUniqueKeyListCount = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 2)
      val bmEntryListTotalSize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 3)
      val bmOffsetListTotalSize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 4)
      val bmNullEntryOffset = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 5)
      val bmNullEntrySize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 6)

      // Get the offset for the different segments in bitmap index file.
      val bmUniqueKeyListOffset = IndexFile.VERSION_LENGTH
      val bmEntryListOffset = bmUniqueKeyListOffset + bmUniqueKeyListTotalSize
      val bmOffsetListOffset = bmEntryListOffset + bmEntryListTotalSize + bmNullEntrySize

      bmUniqueKeyListFiber = BitmapFiber(
        () => loadBmKeyList(fin, bmUniqueKeyListOffset),
        idxPath.toString, BitmapIndexSectionId.keyListSection, 0)
      bmUniqueKeyListCache = WrappedFiberCache(FiberCacheManager.get(bmUniqueKeyListFiber, conf))

      bmEntryListFiber = BitmapFiber(
        () => loadBmEntryList(fin, bmEntryListOffset, bmEntryListTotalSize),
        idxPath.toString, BitmapIndexSectionId.entryListSection, 0)
      bmEntryListCache = WrappedFiberCache(FiberCacheManager.get(bmEntryListFiber, conf))

      bmOffsetListFiber = BitmapFiber(
        () => loadBmOffsetList(fin, bmOffsetListOffset, bmOffsetListTotalSize),
        idxPath.toString, BitmapIndexSectionId.entryOffsetsSection, 0)
      bmOffsetListCache = WrappedFiberCache(FiberCacheManager.get(bmOffsetListFiber, conf))

      bmNullListFiber = BitmapFiber(
        () => loadBmNullList(fin, bmNullEntryOffset, bmNullEntrySize),
        idxPath.toString, BitmapIndexSectionId.entryNullSection, 0)
      bmNullListCache = WrappedFiberCache(FiberCacheManager.get(bmNullListFiber, conf))
    } finally {
      try {
        if (fin != null) {
          fin.close()
        }
      } catch {
        case e: Exception =>
          if (!ShutdownHookManager.inShutdown()) {
            logWarning("Exception in FSDataInputStream.close()", e)
          }
      } finally {
        fin = null
      }
    }
  }

  private def getStartIdxOffset(fiberCache: FiberCache, baseOffset: Long, startIdx: Int): Int = {
    val idxOffset = baseOffset + startIdx * 4
    val startIdxOffset = fiberCache.getInt(idxOffset)
    startIdxOffset
  }

  private def getBitmapIdx(keySeq: IndexedSeq[InternalRow],
      range: RangeInterval): (Int, Int) = {
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

  private def getDesiredBitmaps(byteCache: FiberCache, position: Int,
      startIdx: Int, endIdx: Int): IndexedSeq[RoaringBitmap] = {
    if (byteCache.size() != 0) {
      val bmStream = new BitmapDataInputStream(byteCache)
      bmStream.skipBytes(position)
      (startIdx until endIdx).map( idx => {
        val bmEntry = new RoaringBitmap()
        // Below is directly reading from byte array rather than deserializing into java object.
        bmEntry.deserialize(bmStream)
        bmEntry
      })
    } else {
      IndexedSeq.empty
    }
  }

  private def getDesiredBitmapArray: mutable.ArrayBuffer[RoaringBitmap] = {
    val keySeq = readBmUniqueKeyListFromCache(bmUniqueKeyListCache.fc)
    intervalArray.flatMap{
      case range if !range.isNullPredicate =>
        val (startIdx, endIdx) = getBitmapIdx(keySeq, range)
        if (startIdx == -1 || endIdx == -1) {
          // range not fond in cur bitmap, return empty for performance consideration
          Seq.empty[RoaringBitmap]
        } else {
          val startIdxOffset = getStartIdxOffset(bmOffsetListCache.fc, 0L, startIdx)
          val curPosition = startIdxOffset - IndexFile.VERSION_LENGTH - bmUniqueKeyListTotalSize
          getDesiredBitmaps(bmEntryListCache.fc, curPosition, startIdx, endIdx + 1)
        }
      case range if range.isNullPredicate =>
        getDesiredBitmaps(bmNullListCache.fc, 0, 0, 1)
    }
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
          bitmapArray.reduceLeft(FastAggregation.or(_, _)).iterator.asScala
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
    try {
      initDesiredRowIdIterator()
    } finally {
      clearCache()
    }

    this
  }

  def clearCache(): Unit = {
    if (bmFooterCache != null) {
      bmFooterCache.release()
      bmFooterCache = null
      bmFooterFiber = null
    }
    if (bmUniqueKeyListCache != null) {
      bmUniqueKeyListCache.release()
      bmUniqueKeyListCache = null
      bmUniqueKeyListFiber = null
    }
    if (bmOffsetListCache != null) {
      bmOffsetListCache.release()
      bmOffsetListCache = null
      bmOffsetListFiber = null
    }
    if (bmEntryListCache != null) {
      bmEntryListCache.release()
      bmEntryListCache = null
      bmEntryListFiber = null
    }
    if (bmNullListCache != null) {
      bmNullListCache.release()
      bmNullListCache = null
      bmNullListFiber = null
    }
    bmUniqueKeyListTotalSize = 0
    bmUniqueKeyListCount = 0
  }

  override def toString: String = "BitMapScanner"
}

// Below class is used to directly decode bitmap from FiberCache(either offheap/onheap memory).
private[oap] class BitmapDataInputStream(bitsStream: FiberCache) extends DataInput {

 private val bitsSize: Int = bitsStream.size.toInt
 // The current position to read from FiberCache.
 private var pos: Int = 0

 // The reading byte order is big endian.
 override def readShort(): Short = {
   val curPos = pos
   pos += 2
   (((bitsStream.getByte(curPos) & 0xFF) << 8) |
     ((bitsStream.getByte(curPos + 1) & 0xFF)) & 0xFFFF).toShort
 }

 override def readInt(): Int = {
   val curPos = pos
   pos += 4
   ((bitsStream.getByte(curPos) & 0xFF) << 24) |
     ((bitsStream.getByte(curPos + 1) & 0xFF) << 16) |
     ((bitsStream.getByte(curPos + 2) & 0xFF) << 8) |
     (bitsStream.getByte(curPos + 3) & 0xFF)
 }

 override def readLong(): Long = {
   val curPos = pos
   pos += 8
   ((bitsStream.getByte(curPos) & 0xFF) << 56) |
     ((bitsStream.getByte(curPos + 1) & 0xFF) << 48) |
     ((bitsStream.getByte(curPos + 2) & 0xFF) << 40) |
     ((bitsStream.getByte(curPos + 3) & 0xFF) << 32) |
     ((bitsStream.getByte(curPos + 4) & 0xFF) << 24) |
     ((bitsStream.getByte(curPos + 5) & 0xFF) << 16) |
     ((bitsStream.getByte(curPos + 6) & 0xFF) << 8) |
     (bitsStream.getByte(curPos + 7) & 0xFF)
 }

 override def readFully(readBuffer: Array[Byte], offset: Int, length: Int): Unit = {
   if (length < 0) {
     throw new IndexOutOfBoundsException("read length is inlegal for bitmap index.\n")
   }
   var curPos = pos
   pos += length
   if (pos > bitsSize - 1) {
     throw new EOFException("read is ending of file for bitmap index.\n")
   }
   (offset until (offset + length)).foreach(idx => {
     readBuffer(idx) = bitsStream.getByte(curPos)
     curPos += 1
   })
 }

 override def skipBytes(n: Int): Int = {
   pos += n
   n
 }

 // Below are not needed by roaring bitmap, just implement them for DataInput interface.
 override def readBoolean(): Boolean = {
   val curPos = pos
   pos += 1
   if (bitsStream.getByte(curPos).toInt != 0) true else false
 }

 override def readByte(): Byte = {
   val curPos = pos
   pos += 1
   bitsStream.getByte(curPos)
 }

 override def readUnsignedByte(): Int = {
   readByte().toInt
 }

 override def readUnsignedShort(): Int = {
   readShort().toInt
 }

 override def readChar(): Char = {
   readShort().toChar
 }

 override def readDouble(): Double = {
   readLong().toDouble
 }

 override def readFloat(): Float = {
   readInt().toFloat
 }

 override def readFully(readBuffer: Array[Byte]): Unit = {
   readFully(readBuffer, 0, readBuffer.length)
 }

 override def readLine(): String = {
   throw new UnsupportedOperationException("Bitmap doesn't need this." +
     "It's inlegal to use it in bitmap!!!")
 }

 override def readUTF(): String = {
   throw new UnsupportedOperationException("Bitmap doesn't need this." +
     "It' inlegal to use it in bitmap!!!")
 }
}
