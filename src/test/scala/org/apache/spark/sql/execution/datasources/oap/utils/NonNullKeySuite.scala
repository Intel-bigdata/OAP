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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ByteBufferOutputStream


class NonNullKeySuite extends SparkFunSuite with Logging {

  private lazy val random = new Random(0)
  private lazy val values = {
    val booleans: Seq[Boolean] = Seq(true, false)
    val bytes: Seq[Byte] = Seq(Byte.MinValue, 0, 10, 30, Byte.MaxValue)
    val shorts: Seq[Short] = Seq(Short.MinValue, -100, 0, 10, 200, Short.MaxValue)
    val ints: Seq[Int] = Seq(Int.MinValue, -100, 0, 100, 12346, Int.MaxValue)
    val longs: Seq[Long] = Seq(Long.MinValue, -10000, 0, 20, Long.MaxValue)
    val floats: Seq[Float] = Seq(Float.MinValue, Float.MinPositiveValue, Float.MaxValue)
    val doubles: Seq[Double] = Seq(Double.MinValue, Double.MinPositiveValue, Double.MaxValue)
    val strings: Seq[UTF8String] =
      Seq("", "test", "b plus tree", "BTreeRecordReaderWriter").map(UTF8String.fromString)
    val binaries: Seq[Array[Byte]] = (0 until 20 by 5).map{ size =>
      val buf = new Array[Byte](size)
      random.nextBytes(buf)
      buf
    }
    val values = booleans ++ bytes ++ shorts ++ ints ++ longs ++
      floats ++ doubles ++ strings ++ binaries ++ Nil
    random.shuffle(values)
  }
  private def toSparkDataType(any: Any): DataType = {
    any match {
      case _: Boolean => BooleanType
      case _: Short => ShortType
      case _: Byte => ByteType
      case _: Int => IntegerType
      case _: Long => LongType
      case _: Float => FloatType
      case _: Double => DoubleType
      case _: UTF8String => StringType
      case _: Array[Byte] => BinaryType
    }
  }

  test("Read/Write Based On Schema") {
    values.grouped(10).foreach { valueSeq =>
      val schema = StructType(valueSeq.zipWithIndex.map {
        case (v, i) => StructField(s"col$i", toSparkDataType(v))
      })
      val nnkw = new NonNullKeyWriter(schema)
      val nnkr = new NonNullKeyReader(schema)
      val row = InternalRow.fromSeq(valueSeq)
      val buf = new ByteBufferOutputStream()
      nnkw.writeKey(buf, row)
      val answerRow = nnkr.readKey(FiberCache(buf.toByteArray), 0)._1
      assert(row.equals(answerRow))
    }
  }
}
