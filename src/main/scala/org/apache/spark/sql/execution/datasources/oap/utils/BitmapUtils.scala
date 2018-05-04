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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

private[oap] object BitmapUtils {

  // Below constants are used by OapBitmapWrappedFiberCache class.
  val SERIAL_COOKIE: Int = 12347
  val SERIAL_COOKIE_NO_RUNCONTAINER: Int = 12346
  val NO_OFFSET_THRESHOLD: Int = 4
  val DEFAULT_MAX_SIZE: Int = 4096
  val BITMAP_MAX_CAPACITY: Int = 1 << 16

  def iterator(wfcSeq: Seq[OapBitmapWrappedFiberCache]): Iterator[Int] = {
    // For only one fiber cache, use ChunksInSingleFiberCacheIterator for best performance.
    if (wfcSeq.size == 1) {
      val wfc = wfcSeq.head
      wfc.init
      ChunksInSingleFiberCacheIterator(wfc).prepare
    } else {
      ChunksInMultiFiberCachesIterator(wfcSeq).prepare
    }
  }
}

/**
 * The chunks inside of one single fiber cache are physically consecutive,
 * so it doesn't require to set chunk offset. This class is to directly get
 * the row ID list recorded in all of the chunks of this single fiber cache
 * in ascending order.
 */
private[oap] case class ChunksInSingleFiberCacheIterator(wfc: OapBitmapWrappedFiberCache)
  extends ChunksIterator {

  override def prepare(): Iterator[Int] = {
    totalLength = wfc.getTotalChunkLength
    if (idx < totalLength) {
      iteratorForChunk = wfc.getIteratorForChunk(idx)
      val cks = wfc.getChunkKeys
      highPart = (cks(idx) & 0xFFFF) << 16
    }
    this
  }
}

/**
 * The chunks inside of different multi fiber cache are not physically consecutive, so it
 * requires to set chunk offset for different fibers. This class is to directly get the row ID
 * list recorded in all of the chunks in all of multi fiber caches in ascending order.
 */
private[oap] case class ChunksInMultiFiberCachesIterator(
    wfcSeq: Seq[OapBitmapWrappedFiberCache])
  extends ChunksIterator {

  private val chunksInFc: ArrayBuffer[OapBitmapChunkInFiberCache] = or

  override def prepare(): Iterator[Int] = {
    totalLength = chunksInFc.length
    if (idx < totalLength) {
      val wfc = chunksInFc(idx).wfc
      val chunkIdx = chunksInFc(idx).chunkIdx
      wfc.setOffset(chunkIdx)
      iteratorForChunk = wfc.getIteratorForChunk(chunkIdx)
      highPart = (chunksInFc(idx).getChunkKey & 0xFFFF) << 16
    }
    this
  }

  /**
   * Below method will virtually link all the chunks in multi fiber caches in ascending order
   * of chunk key. It will provide the input for the above ChunksInMultiFiberCachesIterator.
   */
  private def or(): ArrayBuffer[OapBitmapChunkInFiberCache] = {
    val firstWfc = wfcSeq(0)
    firstWfc.init
    val initialChunkLength = firstWfc.getTotalChunkLength
    val finalChunkArray = new ArrayBuffer[OapBitmapChunkInFiberCache]()
    var initialIdx = 0
    var nextIdx = 0
    (0 until initialChunkLength).map(idx => {
      finalChunkArray += OapBitmapChunkInFiberCache(firstWfc, idx)
    })
    (1 until wfcSeq.length).foreach(idx => {
      initialIdx = 0
      var initialKey = finalChunkArray(initialIdx).getChunkKey
      val nextWfc = wfcSeq(idx)
      nextWfc.init
      nextIdx = 0
      val nextChunkLength = nextWfc.getTotalChunkLength
      val nextChunkKeys = nextWfc.getChunkKeys
      var nextKey = nextChunkKeys(nextIdx)
      breakable {
        while (true) {
          val result = initialKey - nextKey
            if (result < 0) {
              initialIdx += 1
              if (initialIdx == finalChunkArray.length) break
              initialKey = finalChunkArray(initialIdx).getChunkKey
            } else if (result == 0) {
              // Just link the next chunk to be adjacent for traversing.
              finalChunkArray.insert(initialIdx, OapBitmapChunkInFiberCache(nextWfc, nextIdx))
              // Bypass the two adjacent chunks with equal keys.
              initialIdx += 2
              nextIdx += 1
              if (initialIdx == finalChunkArray.length || nextIdx == nextChunkLength) break
              initialKey = finalChunkArray(initialIdx).getChunkKey
              nextKey = nextChunkKeys(nextIdx)
            } else if (result > 0) {
              // Insert the next chunk with nextIdx from the next fiber cache.
              finalChunkArray.insert(initialIdx, OapBitmapChunkInFiberCache(nextWfc, nextIdx))
              initialIdx += 1
              nextIdx += 1
              if (nextIdx == nextChunkLength) break
              nextKey = nextChunkKeys(nextIdx)
            }
        }
      }
      if (initialIdx == finalChunkArray.length && nextIdx < nextChunkLength) {
        // Append the remaining chunks from the above next fiber cache.
        (nextIdx until nextChunkLength).foreach(idx =>
          finalChunkArray += OapBitmapChunkInFiberCache(nextWfc, idx))
      }
    })
    finalChunkArray
  }
}
