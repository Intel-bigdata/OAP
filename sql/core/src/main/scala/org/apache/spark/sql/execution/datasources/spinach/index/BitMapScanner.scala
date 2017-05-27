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
package org.apache.spark.sql.execution.datasources.spinach.index

import java.io.{ByteArrayInputStream, ObjectInputStream}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach._
import org.apache.spark.sql.execution.datasources.spinach.filecache._
import org.apache.spark.sql.execution.datasources.spinach.io.IndexFile
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.BitSet

private[spinach] case class BitMapScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = true

  @transient var internalItr: Iterator[Int] = Iterator[Int]()
  var empty: Boolean = _
  var internalBitSet: BitSet = _

  override def hasNext: Boolean = !empty && internalItr.hasNext

  override def next(): Long = internalItr.next().toLong

  override def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    encodeIntervalAndSchema(dataPath, conf)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name)
    val indexScanner = IndexFiber(IndexFile(path))
    val indexData: IndexFiberCacheData = FiberCacheManager(indexScanner, conf)

    val buffer: DataFiberCache = DataFiberCache(indexData.fiberData)
    val baseObj = buffer.fiberData.getBaseObject
    val baseOffset = buffer.fiberData.getBaseOffset

    // get the byte number first
    val objLenth = Platform.getInt(baseObj, baseOffset)
    val byteArrayStart = baseOffset + 4

    // deserialize hashMap[Key: InternalRow, Value: BitSet] from index file
    val byteArray = (0 until objLenth).map(i => {
      Platform.getByte(baseObj, byteArrayStart + i)
    }).toArray
    val inputStream = new ByteArrayInputStream(byteArray)
    val in = new ObjectInputStream(inputStream)
    val hashMap = in.readObject().asInstanceOf[mutable.HashMap[InternalRow, BitSet]]
    this.ordering = GenerateOrdering.create(encodedKeySchema)

    // get sorted key list and generate final bitset
    val sortedKeyList = hashMap.keySet.toList.sorted(this.ordering)
    val bitMapArray = encodedIntervalArray.flatMap(range => {
      val startIdx = if (range.start == IndexScanner.DUMMY_KEY_START) {
        // diff from which startIdx not found, so here startIdx = -2
        -2
      } else {
        // find first key which >= start key, can't find return -1
        if (range.startInclude) {
          sortedKeyList.indexWhere(ordering.compare(range.start, _) <= 0)
        } else {
          sortedKeyList.indexWhere(ordering.compare(range.start, _) < 0)
        }
      }
      val endIdx = if (range.end == IndexScanner.DUMMY_KEY_END) {
        sortedKeyList.size
      } else {
        // find last key which <= end key, can't find return -1
        if (range.endInclude) {
          sortedKeyList.lastIndexWhere(ordering.compare(_, range.end) <= 0)
        } else {
          sortedKeyList.lastIndexWhere(ordering.compare(_, range.end) < 0)
        }
      }

      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Array.empty[BitSet]
      } else {
        sortedKeyList.slice(startIdx, endIdx + 1).map(key =>
          hashMap.get(key).get)
      }
    })

    if (bitMapArray.nonEmpty) {
      internalBitSet = bitMapArray.reduceLeft(_ | _)
      internalItr = internalBitSet.iterator
      empty = false
    } else {
      empty = true
    }

    this
  }

  override def toString: String = "BitMapScanner"
}
