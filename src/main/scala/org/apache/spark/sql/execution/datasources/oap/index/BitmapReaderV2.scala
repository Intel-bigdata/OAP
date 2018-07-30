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

import org.apache.spark.sql.execution.datasources.oap.filecache.BitmapFiberId
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileReaderImpl
import org.apache.spark.sql.execution.datasources.oap.utils.{BitmapUtils, OapBitmapWrappedFiberCache}
import org.apache.spark.sql.types.StructType

private[oap] class BitmapReaderV2(
    fileReader: IndexFileReaderImpl,
    intervalArray: ArrayBuffer[RangeInterval],
    internalLimit: Int,
    keySchema: StructType,
    conf: Configuration)
    extends BitmapReader(fileReader, intervalArray, keySchema, conf) with Iterator[Int] {

  @transient private var bmRowIdIterator: Iterator[Int] = _
  private var bmWfcSeq: Seq[OapBitmapWrappedFiberCache] = _
  private var empty: Boolean = _

 /* V2 is directly using fiber cache. Thus it needs to ensure that the bitmap fiber cache is
  * residing in cache manager before the current query is finished. The current solution is to
  * release the fibers after iterating is finished. However, the side effect for this soluction
  * is to keep the high memory pressure in cache manager.
  * TODO: The solution for the above side effect is to improve the fiber cache manager to free
  * unused cache memory to alleviate the cache eviction by size, and meanwhile keep still using
  * cache memory. Even if it's evicted, put it back into cache as long as it's still using.
  */
  override def hasNext: Boolean =
    if (!empty && bmRowIdIterator.hasNext) {
      true
    } else {
      clearCache()
      false
    }

  override def next(): Int = bmRowIdIterator.next()
  override def toString: String = "BitmapReaderV2"

  override def clearCache(): Unit = {
    super.clearCache()
    if (bmWfcSeq != null) {
      bmWfcSeq.foreach(wfc => wfc.release)
    }
  }

  private def getDesiredWfcSeq(): Seq[OapBitmapWrappedFiberCache] = {
    val keySeq = readBmUniqueKeyList(bmUniqueKeyListCache)
    intervalArray.flatMap{
      case range if !range.isNullPredicate =>
        val (startIdx, endIdx) = getKeyIdx(keySeq, range)
        if (startIdx == -1 || endIdx == -1) {
          Seq.empty
        } else {
          (startIdx until (endIdx + 1)).map(idx => {
            val curIdxOffset = getIdxOffset(bmOffsetListCache, 0L, idx)
            val entrySize = getIdxOffset(bmOffsetListCache, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiberId(() => fileReader.readFiberCache(curIdxOffset, entrySize),
              fileReader.getName, BitmapIndexSectionId.entryListSection, idx)
            new OapBitmapWrappedFiberCache(fiberCacheManager.get(entryFiber))
          })
        }
      case range if range.isNullPredicate =>
        val nullListCache =
          new OapBitmapWrappedFiberCache(fiberCacheManager.get(bmNullListFiber))
        if (nullListCache.size != 0) {
          Seq(nullListCache)
        } else {
          Seq.empty
        }
    }
  }

  def initRowIdIterator(): Unit = {
    initDesiredSegments()
    bmWfcSeq = getDesiredWfcSeq
    if (bmWfcSeq.nonEmpty) {
      val iterator = BitmapUtils.iterator(bmWfcSeq)
      bmRowIdIterator =
        if (internalLimit > 0) iterator.take(internalLimit) else iterator
      empty = false
    } else {
      empty = true
    }
  }
}
