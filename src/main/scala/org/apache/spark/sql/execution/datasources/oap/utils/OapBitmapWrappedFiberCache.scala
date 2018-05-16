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

package org.apache.spark.sql.execution.datasources.oap.utils

import scala.util.control.Breaks._

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache

// Oap Bitmap Scanner will use below class to directly traverse the row IDs
// in fiber caches while keeping compatible with Roaring Bitmap format.
// It doesn't need to create the roaring bitmap object on heap any more.
// The spec link is https://github.com/RoaringBitmap/RoaringFormatSpec/
private[oap] class OapBitmapWrappedFiberCache(fc: FiberCache) {

  private var chunkLength: Int = 0
  // It indicates no this section.
  private var chunkOffsetListOffset: Int = -1

  private var curOffset: Int = 0

  // No run chunks by default.
  private var hasRun: Boolean = false
  private var bitmapOfRunChunks: Array[Byte] = _

  private var chunkKeys: Array[Short] = _
  private var chunkCardinalities: Array[Short] = _

  // The reading byte order is little endian to keep consistent with roaring bitmap writing order.
  // Read from the specific offset.
  private def getIntNoMoving(offset: Int): Int = {
    ((fc.getByte(offset + 3) & 0xFF) << 24) |
      ((fc.getByte(offset + 2) & 0xFF) << 16) |
      ((fc.getByte(offset + 1) & 0xFF) << 8) |
      (fc.getByte(offset) & 0xFF)
  }

  private def getChunkSize(chunkIdx: Int): Int = {
    // NO run chunks in this case.
    chunkIdx match {
      case idx if isArrayChunk(idx) =>
        (chunkCardinalities(idx) & 0xFFFF) * 2
      case idx if isBitmapChunk(idx) =>
        BitmapUtils.BITMAP_MAX_CAPACITY / 8
      case _ =>
        throw new OapException("It's illegal to get chunk size.")
    }
  }

  private def getBytes(length: Int): Array[Byte] = {
    val byteBuffer = fc.getBytes(curOffset, length)
    curOffset += length
    byteBuffer
  }

  private def isRunChunk(idx: Int): Boolean = {
    if (hasRun && (bitmapOfRunChunks(idx / 8) & (1 << (idx % 8))).toInt != 0) true else false
  }

  private def isArrayChunk(idx: Int): Boolean = {
    // The logic in getIteratorForChunk excludes the run chunk first.
    (chunkCardinalities(idx) & 0xFFFF) <= BitmapUtils.DEFAULT_MAX_SIZE
  }

  private def isBitmapChunk(idx: Int): Boolean = {
    // The logic in getIteratorForChunk excludes the run and array chunks first.
    (chunkCardinalities(idx) & 0xFFFF) > BitmapUtils.DEFAULT_MAX_SIZE
  }

  private def getInt(): Int = {
    val curPos = curOffset
    curOffset += 4
    ((fc.getByte(curPos + 3) & 0xFF) << 24) |
      ((fc.getByte(curPos + 2) & 0xFF) << 16) |
      ((fc.getByte(curPos + 1) & 0xFF) << 8) |
      (fc.getByte(curPos) & 0xFF)
  }

  def release(): Unit = fc.release

  def getShort(): Short = {
    val curPos = curOffset
    curOffset += 2
    (((fc.getByte(curPos + 1) & 0xFF) << 8) |
      (fc.getByte(curPos) & 0xFF)).toShort
  }

  def getLong(): Long = {
    val curPos = curOffset
    curOffset += 8
    ((fc.getByte(curPos + 7) & 0xFFL) << 56) |
      ((fc.getByte(curPos + 6) & 0xFFL) << 48) |
      ((fc.getByte(curPos + 5) & 0xFFL) << 40) |
      ((fc.getByte(curPos + 4) & 0xFFL) << 32) |
      ((fc.getByte(curPos + 3) & 0xFFL) << 24) |
      ((fc.getByte(curPos + 2) & 0xFFL) << 16) |
      ((fc.getByte(curPos + 1) & 0xFFL) << 8) |
      (fc.getByte(curPos) & 0xFFL)
  }

  def getTotalChunkLength(): Int = chunkLength

  def getChunkKeys(): Array[Short] = chunkKeys

  def getChunkCardinality(idx: Int): Short = chunkCardinalities(idx)

  def getBitmapCapacity(): Int = BitmapUtils.BITMAP_MAX_CAPACITY

  // It's used to traverse multi-chunks across multi-fiber caches in bitwise OR case.
  def setOffset(chunkIdx: Int): Unit = {
    if (chunkOffsetListOffset < 0) {
      var accumulOffset = 0
      (0 until chunkIdx).foreach(idx =>
        accumulOffset += getChunkSize(idx))
      curOffset += accumulOffset
    } else {
      curOffset = getIntNoMoving(chunkOffsetListOffset + chunkIdx * 4)
    }
  }

  def getIteratorForChunk(chunkIdx: Int): Iterator[Int] = {
    chunkIdx match {
      case idx if isRunChunk(idx) =>
        RunChunkIterator(this)
      case idx if isArrayChunk(idx) =>
        ArrayChunkIterator(this, idx)
      case idx if isBitmapChunk(idx) =>
        BitmapChunkIterator(this)
      case _ =>
        throw new OapException("It's illegal chunk in bitmap index fiber caches.\n")
    }
  }

  def init(): Unit = {
    val cookie = getInt
    cookie match {
      case ck if ((ck & 0xFFFF) == BitmapUtils.SERIAL_COOKIE) =>
        chunkLength = (cookie >>> 16) + 1
        hasRun = true
      case ck if (ck == BitmapUtils.SERIAL_COOKIE_NO_RUNCONTAINER) =>
        chunkLength = getInt
      case _ =>
        throw new OapException("It's invalid roaring bitmap header in OAP bitmap index file.")
    }
    if (hasRun) {
      val size = (chunkLength + 7) / 8
      bitmapOfRunChunks = getBytes(size)
    }
    chunkKeys = new Array[Short](chunkLength)
    chunkCardinalities = new Array[Short](chunkLength)
    (0 until chunkLength).foreach(idx => {
      chunkKeys(idx) = getShort
      chunkCardinalities(idx) = (1 + (0xFFFF & getShort)).toShort
    })
    if (!hasRun || chunkLength >= BitmapUtils.NO_OFFSET_THRESHOLD) {
      chunkOffsetListOffset = curOffset
      curOffset += chunkLength * 4
    }
  }
}

private[oap] case class OapBitmapChunkInFiberCache(wfc: OapBitmapWrappedFiberCache, chunkIdx: Int) {
  def getChunkKey(): Short = {
    val cks = wfc.getChunkKeys
    cks(chunkIdx)
  }
}

private[oap] abstract class ChunksIterator extends Iterator[Int] {

  protected var idx: Int = 0
  protected var totalLength: Int = 0
  protected var highPart: Int = 0
  protected var iteratorForChunk: Iterator[Int] = _

  def prepare(): Iterator[Int]

  override def hasNext: Boolean = idx < totalLength

  override def next(): Int = {
    val value = iteratorForChunk.next | highPart
    if (!iteratorForChunk.hasNext) {
      idx += 1
      prepare
    }
    value
  }
}

private[oap] case class RunChunkIterator(wfc: OapBitmapWrappedFiberCache) extends Iterator[Int] {

  private val totalRuns: Int = wfc.getShort & 0xFFFF
  private var runBase: Int = wfc.getShort & 0xFFFF
  private var maxCurRunLength: Int = wfc.getShort & 0xFFFF
  private var runIdx: Int = 0
  private var runLength: Int = 0

  override def hasNext: Boolean = runIdx < totalRuns

  override def next(): Int = {
    val value = runBase + runLength
    runLength += 1
    if (runLength > maxCurRunLength) {
      runLength = 0
      runIdx += 1
      if (runIdx < totalRuns) {
        runBase = wfc.getShort & 0xFFFF
        maxCurRunLength = wfc.getShort & 0xFFFF
      }
    }
    value
  }
}

// Chunk index is used to get the cardinality for array chunk.
private[oap] case class ArrayChunkIterator(wfc: OapBitmapWrappedFiberCache, chunkIdx: Int)
  extends Iterator[Int] {

  private val totalCountInChunk: Int = wfc.getChunkCardinality(chunkIdx) & 0xFFFF
  private var idxInChunk: Int = 0

  override def hasNext: Boolean = idxInChunk < totalCountInChunk

  override def next(): Int = {
    idxInChunk += 1
    wfc.getShort & 0xFFFF
  }
}

private[oap] case class BitmapChunkIterator(wfc: OapBitmapWrappedFiberCache) extends Iterator[Int] {

  private val wordLength: Int = wfc.getBitmapCapacity / 64
  private var wordIdx: Int = -1
  private var curWord: Long = 0L
  do {
    curWord = wfc.getLong
    wordIdx += 1
  } while (curWord == 0L && wordIdx < wordLength)

  override def hasNext: Boolean = wordIdx < wordLength

  override def next(): Int = {
    val tmp = curWord & -curWord
    val value = wordIdx * 64 + java.lang.Long.bitCount(tmp - 1)
    curWord ^= tmp
    breakable {
      while (curWord == 0L) {
        wordIdx += 1
        if (wordIdx == wordLength) break
        curWord = wfc.getLong
      }
    }
    value
  }
}
