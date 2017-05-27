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

package org.apache.spark.sql.execution.datasources.spinach

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.spinach.index.{BloomFilter, IndexOutputWriter, IndexUtils, RangeInterval}
import org.apache.spark.sql.execution.datasources.spinach.statistics.{BloomFilterStatistics, BloomFilterStatisticsType, StaticsAnalysisResult}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

class TestIndexOutputWriter extends IndexOutputWriter(bucketId = None, context = null) {
  val buf = new ByteArrayOutputStream(8000)
  override protected lazy val writer: RecordWriter[Void, Any] =
    new RecordWriter[Void, Any] {
      override def close(context: TaskAttemptContext) = buf.close()
      override def write(key: Void, value: Any) = value match {
        case bytes: Array[Byte] => buf.write(bytes)
        case i: Int => buf.write(i) // this will only write a byte
      }
    }
}

class BloomFilterStatisticsSuite extends SparkFunSuite {
  def rowGen(i: Int): InternalRow = InternalRow(i, UTF8String.fromString(s"test#$i"))

  test("write function test") {
    val out = new TestIndexOutputWriter
    val schema = StructType(StructField("a", IntegerType) :: StructField("b", StringType) :: Nil)

    val keys = (1 to 300).map(i => rowGen(i)).toArray

    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    val projector = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray

    var elementCnt = 0
    val bfIndex = new BloomFilter(1 << 20, 3)()

    keys.foreach(key => {
      elementCnt += 1
      projector.foreach(p => bfIndex.addValue(p(key).getBytes))
    })
    val bfLongArray = bfIndex.getBitMapLongArray

    val statistics = new BloomFilterStatistics
    statistics.initialize(1 << 20, 3)
    statistics.write(schema, out, keys, null, null)

    val bytes = out.buf.toByteArray

    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET) == BloomFilterStatisticsType.id)
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 4) == bfLongArray.length)
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 8) == 3)
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 12) == elementCnt)

    var offset = 16

    for (i <- bfLongArray.indices) {
      val longFromFile = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      assert(longFromFile == bfLongArray(i))
      offset += 8
    }
  }

  test("read function test") {
    val out = new TestIndexOutputWriter
    val schema = StructType(StructField("a", IntegerType) :: StructField("b", StringType) :: Nil)

    val keys = (1 to 300).map(i => rowGen(i)).toArray

    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    val projector = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray

    var elementCnt = 0
    val bfIndex = new BloomFilter(1 << 20, 3)()

    keys.foreach(key => {
      elementCnt += 1
      projector.foreach(p => bfIndex.addValue(p(key).getBytes))
    })
    val bfLongArray = bfIndex.getBitMapLongArray

    IndexUtils.writeInt(out, BloomFilterStatisticsType.id)
    IndexUtils.writeInt(out, bfLongArray.length)
    IndexUtils.writeInt(out, bfIndex.getNumOfHashFunc)
    IndexUtils.writeInt(out, elementCnt)

    bfLongArray.foreach(l => IndexUtils.writeLong(out, l))

    val intervalArray: ArrayBuffer[RangeInterval] = new ArrayBuffer[RangeInterval]()
    intervalArray.append(new RangeInterval(rowGen(-101), rowGen(-101),
      includeStart = true, includeEnd = true))

    val statistics = new BloomFilterStatistics
    val res = statistics.read(schema, intervalArray, out.buf.toByteArray, 0)

    assert(res == StaticsAnalysisResult.SKIP_INDEX)
  }

  test("read AND write test") {
    val out = new TestIndexOutputWriter
    val schema = StructType(StructField("a", IntegerType) :: StructField("b", StringType) :: Nil)

    val keys = (1 to 300).map(i => rowGen(i)).toArray

    val statistics = new BloomFilterStatistics
    statistics.initialize(1 << 20, 3)
    statistics.write(schema, out, keys, null, null)

    val intervalArray: ArrayBuffer[RangeInterval] = new ArrayBuffer[RangeInterval]()
    intervalArray.append(new RangeInterval(rowGen(-101), rowGen(-101),
      includeStart = true, includeEnd = true))

    val res = statistics.read(schema, intervalArray, out.buf.toByteArray, 0)
    assert(res == StaticsAnalysisResult.SKIP_INDEX)
  }
}
