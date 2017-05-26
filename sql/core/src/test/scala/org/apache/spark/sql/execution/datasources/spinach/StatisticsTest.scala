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
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.{BaseOrdering, GenerateOrdering}
import org.apache.spark.sql.execution.datasources.spinach.index.{BloomFilter, IndexOutputWriter, RangeInterval}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

abstract class StatisticsTest extends SparkFunSuite with BeforeAndAfterEach {
  protected class TestIndexOutputWriter extends IndexOutputWriter(bucketId = None, context = null) {
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

  protected def rowGen(i: Int): InternalRow = InternalRow(i, UTF8String.fromString(s"test#$i"))

  protected var schema: StructType = StructType(StructField("a", IntegerType)
    :: StructField("b", StringType) :: Nil)

  @transient lazy protected val converter: UnsafeProjection = UnsafeProjection.create(schema)
  @transient lazy protected val ordering: BaseOrdering = GenerateOrdering.create(schema)
  protected var out: TestIndexOutputWriter = _

  protected var intervalArray: ArrayBuffer[RangeInterval] = new ArrayBuffer[RangeInterval]()

  override def beforeEach(): Unit = {
    out = new TestIndexOutputWriter
  }

  override def afterEach(): Unit = {
    out.close()
    intervalArray.clear()
  }

  protected def generateInterval(start: InternalRow, end: InternalRow,
                                 startInclude: Boolean, endInclude: Boolean) = {
    intervalArray.clear()
    intervalArray.append(new RangeInterval(start, end, startInclude, endInclude))
  }

  protected def checkInternalRow(row1: InternalRow, row2: InternalRow) = {
    val res = row1 == row2 // it works..
    assert(res, s"row1: $row1 does not match $row2")
  }

  protected def checkBloomFilter(bf1: BloomFilter, bf2: BloomFilter) = {
    val res =
      if (bf1.getNumOfHashFunc != bf2.getNumOfHashFunc) false
      else {
        val bitLongArray1 = bf1.getBitMapLongArray
        val bitLongArray2 = bf2.getBitMapLongArray

        bitLongArray1.length == bitLongArray2.length && bitLongArray1.zip(bitLongArray2)
          .map(t => t._1 == t._2).reduceOption(_ && _).getOrElse(true)
      }
    assert(res, "bloom filter does not match")
  }
}
