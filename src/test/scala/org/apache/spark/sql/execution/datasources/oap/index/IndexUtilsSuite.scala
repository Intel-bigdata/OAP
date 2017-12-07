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

import java.io.{ByteArrayOutputStream, DataOutputStream}

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.junit.Assert._

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ByteBufferOutputStream


class IndexUtilsSuite extends SparkFunSuite with Logging {
  test("write int to unsafe") {
    val buf = new ByteArrayOutputStream(8)
    val out = new DataOutputStream(buf)
    IndexUtils.writeInt(out, -19)
    IndexUtils.writeInt(out, 4321)
    val bytes = buf.toByteArray
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET) == -19)
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 4) == 4321)
  }

  test("write long to IndexOutputWriter") {
    val buf = new ByteArrayOutputStream(32)
    val out = new DataOutputStream(buf)
    IndexUtils.writeLong(out, -19)
    IndexUtils.writeLong(out, 4321)
    IndexUtils.writeLong(out, 43210912381723L)
    IndexUtils.writeLong(out, -99128917321912L)
    out.close()
    val bytes = buf.toByteArray
    assert(Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET) == -19)
    assert(Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + 8) == 4321)
    assert(Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + 16) == 43210912381723L)
    assert(Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + 24) == -99128917321912L)
  }

  test("index path generating") {
    assertEquals("/path/to/.t1.ABC.index1.index",
      IndexUtils.indexFileFromDataFile(new Path("/path/to/t1.data"), "index1", "ABC").toString)
    assertEquals("/.t1.1F23.index1.index",
      IndexUtils.indexFileFromDataFile(new Path("/t1.data"), "index1", "1F23").toString)
    assertEquals("/path/to/.t1.0.index1.index",
      IndexUtils.indexFileFromDataFile(new Path("/path/to/t1.parquet"), "index1", "0").toString)
    assertEquals("/path/to/.t1.F91.index1.index",
      IndexUtils.indexFileFromDataFile(new Path("/path/to/t1"), "index1", "F91").toString)
  }

  test("get index work file path") {
    assertEquals("/path/to/_temp/0/.t1.ABC.index1.index",
      IndexUtils.getIndexWorkPath(
        new Path("/path/to/.t1.data"),
        new Path("/path/to"),
        new Path("/path/to/_temp/0"),
        ".t1.ABC.index1.index").toString)
    assertEquals("hdfs:/path/to/_temp/1/a=3/b=4/.t1.ABC.index1.index",
      IndexUtils.getIndexWorkPath(
        new Path("hdfs:/path/to/a=3/b=4/.t1.data"),
        new Path("/path/to"),
        new Path("/path/to/_temp/1"),
        ".t1.ABC.index1.index").toString)
    assertEquals("hdfs://remote:8020/path/to/_temp/2/x=1/.t1.ABC.index1.index",
      IndexUtils.getIndexWorkPath(
        new Path("hdfs://remote:8020/path/to/x=1/.t1.data"),
        new Path("/path/to/"),
        new Path("/path/to/_temp/2/"),
        ".t1.ABC.index1.index").toString)
  }

  test("writeHead to write common and consistent index version to all the index file headers") {
    val buf = new ByteArrayOutputStream(8)
    val out = new DataOutputStream(buf)
    IndexUtils.writeHead(out, IndexFile.INDEX_VERSION)
    val bytes = buf.toByteArray
    assert(Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + 6) ==
      (IndexFile.INDEX_VERSION >> 8).toByte)
    assert(Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + 7) ==
      (IndexFile.INDEX_VERSION & 0xFF).toByte)
  }

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
      val row = InternalRow.fromSeq(valueSeq)
      val buf = new ByteBufferOutputStream()
      IndexUtils.writeBasedOnSchema(buf, row, schema)
      val answerRow = IndexUtils.readBasedOnSchema(
        FiberCache(buf.toByteArray), 0L, schema)
      assert(row.equals(answerRow))
    }
  }

  test("Read/Write Based On Data Type") {
    values.foreach { value =>
      val buf = new ByteBufferOutputStream()
      IndexUtils.writeBasedOnDataType(buf, value)

      val (answerValue, offset) = IndexUtils.readBasedOnDataType(
        FiberCache(buf.toByteArray), 0L, toSparkDataType(value))

      assert(value === answerValue, s"value: $value")
      value match {
        case x: UTF8String =>
          assert(offset === x.getBytes.length + IndexUtils.INT_SIZE, s"string: $x")
        case y: Array[Byte] =>
          assert(offset === y.length + IndexUtils.INT_SIZE, s"binary: ${y.mkString(",")}")
        case other =>
          assert(offset === toSparkDataType(other).defaultSize, s"value $other")
      }
    }
  }
}
