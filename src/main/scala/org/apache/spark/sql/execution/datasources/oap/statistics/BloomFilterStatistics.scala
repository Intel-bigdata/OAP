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

package org.apache.spark.sql.execution.datasources.oap.statistics

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

private[oap] class BloomFilterStatistics extends Statistics {
  override val id: Int = BloomFilterStatisticsType.id

  protected var bfIndex: BloomFilter = _

  private lazy val bfMaxBits: Int = StatisticsManager.bloomFilterMaxBits
  private lazy val bfHashFuncs: Int = StatisticsManager.bloomFilterHashFuncs

  @transient private var projectors: Array[UnsafeProjection] = _ // for write
  @transient private lazy val ordering = GenerateOrdering.create(schema)

  override def initialize(schema: StructType): Unit = {
    super.initialize(schema)
    bfIndex = new BloomFilter(bfMaxBits, bfHashFuncs)()
    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    // for multi-column index, add all subsets into bloom filter
    // For example, a column with a = 1, b = 2, a and b are index columns
    // then three records: a = 1, b = 2, a = 1 b = 2, are inserted to bf
    projectors = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray
  }

  override def addOapKey(key: Key): Unit = {
    assert(bfIndex != null, "Please initialize the statistics")
    projectors.foreach(p => bfIndex.addValue(p(key).getBytes))
  }

  override def write(writer: IndexOutputWriter, sortedKeys: ArrayBuffer[Key]): Long = {
    var offset = super.write(writer, sortedKeys)

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
    val bfBitArray = bfIndex.getBitMapLongArray
    IndexUtils.writeInt(writer, bfBitArray.length) // bfBitArray length
    IndexUtils.writeInt(writer, bfIndex.getNumOfHashFunc) // numOfHashFunc
    offset += 8
    bfBitArray.foreach(l => {
      IndexUtils.writeLong(writer, l)
      offset += 8
    })
    offset
  }

  override def read(bytes: Array[Byte], baseOffset: Long): Long = {
    var offset = super.read(bytes, baseOffset) + baseOffset
    offset += readBloomFilter(bytes, offset)
    offset - baseOffset
  }

  private def readBloomFilter(bytes: Array[Byte], baseOffset: Long): Long = {
    var offset = baseOffset
    val bitLength = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    val numHashFunc = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 4)
    offset += 8

    val bitSet = new Array[Long](bitLength)

    for (i <- 0 until bitLength) {
      bitSet(i) = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      offset += 8
    }

    bfIndex = BloomFilter(bitSet, numHashFunc)
    offset - baseOffset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {

    val converter = UnsafeProjection.create(schema)
    val partialSchema = StructType(schema.slice(0, schema.length - 1))
    val partialConverter = UnsafeProjection.create(partialSchema)
    /**
     * When to use Index? If any interval in intervalArray satisfies any of below:
     * 1. If start or end is DUMMY_KEY
     *      Means there is a range interval and bloom filter can't deal with it
     * 2. If interval.start equals interval.end, and they are exists in bfIndex.
     *      In this case, interval.start.numFields == interval.end.numFields
     * 3. If not, Check (schema.length - 1) fields in start OR end exists in bfIndex.
     *      In this case, first (schema.length -1) fields in start and end must be equal.
     * Why?
     * According index selection, if interval has multiple columns,
     * the first (schema.length -1) fields in start and end must be equal.
     */
    val useIndex = intervalArray.isEmpty || intervalArray.exists { interval =>
      val numFields = math.min(interval.start.numFields, interval.end.numFields)
      if (numFields == 0) true
      else if (numFields == schema.length && ordering.eq(interval.start, interval.end)) {
        bfIndex.checkExist(converter(interval.start).getBytes)
      } else {
        bfIndex.checkExist(partialConverter(interval.start).getBytes)
      }
    }

    if (useIndex) StaticsAnalysisResult.USE_INDEX
    else StaticsAnalysisResult.SKIP_INDEX
  }
}
