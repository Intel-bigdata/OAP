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

package org.apache.spark.sql.execution.datasources.spinach.statistics

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.index._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

class BloomFilterStatistics extends Statistics {
  override val id: Int = BloomFilterStatisticsType.id
  override var arrayOffset: Long = _
  private var schema: StructType = _

  private var bfIndex: BloomFilter = _

  def initialize(maxBits: Int, numOfHashFunc: Int): Unit = {
    bfIndex = new BloomFilter(maxBits, numOfHashFunc)()
  }

  private def buildBloomFilter(uniqueKeys: Array[InternalRow]): Int = {
    var elemCnt = 0 // element count
    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    // for multi-column index, add all subsets into bloom filter
    // For example, a column with a = 1, b = 2, a and b are index columns
    // then three records: a = 1, b = 2, a = 1 b = 2, are inserted to bf
    val projector = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray
    for (row <- uniqueKeys) {
      elemCnt += 1
      projector.foreach(p => bfIndex.addValue(p(row).getBytes))
    }
    elemCnt
  }

  // TODO write function parameters need to be optimized
  def write(s: StructType, writer: IndexOutputWriter, uniqueKeys: Array[InternalRow],
            hashMap: java.util.HashMap[InternalRow, java.util.ArrayList[Long]],
            offsetMap: java.util.HashMap[InternalRow, Long]): Unit = {
    arrayOffset = 0
    schema = s
    IndexUtils.writeInt(writer, id)
    arrayOffset += 4
    val elemCnt = buildBloomFilter(uniqueKeys)

    // Bloom filter index file format:
    // numOfLong            4 Bytes, Int, record the total number of Longs in bit array
    // numOfHashFunction    4 Bytes, Int, record the total number of Hash Functions
    // elementCount         4 Bytes, Int, number of elements stored in the
    //                      related DataFile
    //
    // long 1               8 Bytes, Long, the first element in bit array
    // long 2               8 Bytes, Long, the second element in bit array
    // ...
    // long $numOfLong      8 Bytes, Long, the $numOfLong -th element in bit array
    //
    // dataEndOffset        8 Bytes, Long, data end offset
    // rootOffset           8 Bytes, Long, root Offset
    val bfBitArray = bfIndex.getBitMapLongArray
    var offset = 0L
    IndexUtils.writeInt(writer, bfBitArray.length) // bfBitArray length
    IndexUtils.writeInt(writer, bfIndex.getNumOfHashFunc) // numOfHashFunc
    IndexUtils.writeInt(writer, elemCnt)
    offset += 12
    bfBitArray.foreach(l => {
      IndexUtils.writeLong(writer, l)
      offset += 8
    })
    arrayOffset += offset
  }

  private def readBloomFilter(bytes: Array[Byte], startOffset: Long): Long = {
    var offset = startOffset
    val bitLength = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    val numHashFunc = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 4)
    val numOfElems = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 8)
    offset += 12

    val bitSet = new Array[Long](bitLength)

    for (i <- 0 until bitLength) {
      bitSet(i) = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      offset += 8
    }

    bfIndex = BloomFilter(bitSet, numHashFunc)
    offset - startOffset
  }

  // TODO parameter needs to be optimized
  def read(s: StructType, intervalArray: ArrayBuffer[RangeInterval],
           bytes: Array[Byte], baseOffset: Long): Double = {
    val idFromFile = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + baseOffset)
    assert(idFromFile == this.id)
    schema = s
    arrayOffset = baseOffset + 4
    arrayOffset += readBloomFilter(bytes, baseOffset + 4)

    val projector = UnsafeProjection.create(schema)

    val order = GenerateOrdering.create(schema)

    // extract equal condition from intervalArray
    val equalValues: Array[InternalRow] =
      if (intervalArray.nonEmpty) {
        // should not use ordering.compare here
        intervalArray.filter(interval => order.compare(interval.start, interval.end) == 0
          && interval.startInclude && interval.endInclude).map(_.start).toArray
      } else null
    val skipFlag = if (equalValues != null && equalValues.length > 0) {
      !equalValues.map(value => bfIndex
        .checkExist(projector(value).getBytes))
        .reduceOption(_ || _).getOrElse(false)
    } else false

    if (skipFlag) StaticsAnalysisResult.SKIP_INDEX
    else StaticsAnalysisResult.FULL_SCAN
  }
}
