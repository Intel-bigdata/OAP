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
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.bytes.LittleEndianDataOutputStream

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ByteBufferOutputStream, Utils}

class BTreeRecordReaderWriterSuite extends SparkFunSuite {

  private val random = new Random(0)
  private val values = {
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

  test("Read/Write Based On Data Type") {
    values.foreach { value =>
      val buf = new ByteBufferOutputStream()
      val writer = new LittleEndianDataOutputStream(buf)
      BTreeIndexRecordWriter.writeBasedOnDataType(writer, value)

      val (answerValue, offset) = BTreeIndexRecordReader.readBasedOnDataType(
        buf.toByteArray, 0, toSparkDataType(value))

      assert(value === answerValue, s"value: $value")
      value match {
        case x: UTF8String =>
          assert(offset === x.getBytes.length + Integer.BYTES, s"string: $x")
        case y: Array[Byte] =>
          assert(offset === y.length + Integer.BYTES, s"binary: ${y.mkString(",")}")
        case other =>
          assert(offset === toSparkDataType(other).defaultSize, s"value $other")
      }
    }
  }

  test("Read/Write Based On Schema") {
    values.grouped(10).foreach { valueSeq =>
      val schema = StructType(valueSeq.zipWithIndex.map {
        case (v, i) => StructField(s"col$i", toSparkDataType(v))
      })
      val row = InternalRow.fromSeq(valueSeq)
      val buf = new ByteBufferOutputStream()
      val writer = new LittleEndianDataOutputStream(buf)
      BTreeIndexRecordWriter.writeBasedOnSchema(writer, row, schema)
      val answerRow = BTreeIndexRecordReader.readBasedOnSchema(
        buf.toByteArray, 0, schema)
      assert(row.equals(answerRow))
    }
  }

  /**
   * This class is used to inject into BTreeRecordWriter and get the data it write.
   */
  private class TestBTreeIndexFileWriter(conf: Configuration)
      extends BTreeIndexFileWriter(conf, new Path(Utils.createTempDir().getAbsolutePath, "temp")) {
    val nodes = new ArrayBuffer[Array[Byte]]()
    var footer: Array[Byte] = _
    var rowIdList: Array[Byte] = _
    override def start(): Unit = {}
    override def end(): Unit = {}
    override def close(): Unit = {}
    override def writeNode(buf: Array[Byte]): Unit = nodes.append(buf)
    override def writeRowIdList(buf: Array[Byte]): Unit = rowIdList = buf
    override def writeFooter(buf: Array[Byte]): Unit = footer = buf
  }
  // Only test simple Int type since read/write based on schema can cover data type test
  private val schema = StructType(StructField("col", IntegerType) :: Nil)
  private val records = (0 until 1000).map(_ => random.nextInt(1000 / 2))
  private val fileWriter = {
    val configuration = new Configuration()
    val fileWriter = new TestBTreeIndexFileWriter(configuration)
    val writer = BTreeIndexRecordWriter(configuration, new ByteBufferOutputStream(), schema)
    writer.setFileWriter(fileWriter)
    records.map(InternalRow(_)).foreach(writer.write(null, _))
    writer.flush()
    fileWriter.close()
    fileWriter
  }

  test("check read/write nodes") {
    // answer stores sorted unique key list, and the start pos in (sorted) row id list
    val answer = fileWriter.nodes.flatMap { buf =>
      val node = BTreeIndexRecordReader.BTreeNodeData(buf)
      (0 until node.getKeyCount).map(i => (node.getRowIdPos(i), node.getKey(i, schema).getInt(0)))
    }
    assert(answer === records.sorted.distinct.map(v => (records.sorted.indexOf(v), v)))
  }

  test("check read/write rowIdList") {
    val rowIdList = BTreeIndexRecordReader.BTreeRowIdList(fileWriter.rowIdList)
    assert(records.sorted === records.indices.map(rowIdList.getRowId).map(records(_)))
  }

  test("check read/write footer") {
    val footer = BTreeIndexRecordReader.BTreeFooter(fileWriter.footer)
    val nodeCount = footer.getNodesCount
    assert(footer.getRecordCount === records.size)
    assert(nodeCount === fileWriter.nodes.size)

    val nodeSizeSeq = (0 until nodeCount).map(footer.getNodeSize)
    val nodeOffsetSeq = (0 until nodeCount).map(footer.getNodeOffset)
    assert(nodeSizeSeq === fileWriter.nodes.map(_.length))
    assert(nodeOffsetSeq === nodeSizeSeq.scanLeft(0)(_ + _).dropRight(1))

    val keyOffsetSeq = (0 until nodeCount).map ( i =>
      BTreeIndexRecordReader.BTreeNodeData(fileWriter.nodes(i)).getKeyCount
    ).scanLeft(0)(_ + _)
    val uniqueValues = records.sorted.distinct
    (0 until nodeCount).foreach { i =>
      assert(uniqueValues(keyOffsetSeq(i)) === footer.getMinValue(i, schema).getInt(0))
      assert(uniqueValues(keyOffsetSeq(i + 1) - 1) === footer.getMaxValue(i, schema).getInt(0))
    }
  }
}
