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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ByteBufferOutputStream

class StatisticsSuite extends QueryTest with SharedSQLContext with BeforeAndAfterAll{

  var schema: StructType = _
  private lazy val converter = UnsafeProjection.create(schema)
  private lazy val ordering = GenerateOrdering.create(schema)

  override def beforeAll(): Unit = {
    super.beforeAll()
    schema = StructType(StructField("test", DoubleType) :: Nil)
  }

  // an adapter from internalRow to unsafeRow
  private def internalRow2unsafeRow(internalRow: InternalRow): UnsafeRow = converter(internalRow)

  val row1 = InternalRow(1.0)
  val row2 = InternalRow(2.0)
  val row3 = InternalRow(3.0)

  test("rowInSingleInterval: normal test") {
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(row1, row3, true, true), ordering), "2.0 is in [1.0, 3.0]")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row3),
      RangeInterval(row1, row2, true, true), ordering), "3.0 is not in [1.0, 2.0]")
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row1),
      RangeInterval(RangeScanner.DUMMY_KEY_START, row2, false, true), ordering),
      "1.0 is in (-inf, 2.0]")
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(RangeScanner.DUMMY_KEY_START, row2, false, true), ordering),
      "2.0 is in (-inf, 2.0]")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(RangeScanner.DUMMY_KEY_START, row2, false, false), ordering),
      "2.0 is not in (-inf, 2.0)")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row3),
      RangeInterval(RangeScanner.DUMMY_KEY_START, row2, false, true), ordering),
      "3.0 is not in (-inf, 2.0]")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row1),
      RangeInterval(row2, RangeScanner.DUMMY_KEY_END, true, false), ordering),
      "1.0 is in [2, +inf)")
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(row2, RangeScanner.DUMMY_KEY_END, true, false), ordering),
      "2.0 is in [2, +inf)")
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(row2, RangeScanner.DUMMY_KEY_END, false, false), ordering),
      "2.0 is in (2, +inf)")
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row3),
      RangeInterval(row2, RangeScanner.DUMMY_KEY_END, true, false), ordering),
      "3.0 is in [2, +inf)")
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row3),
      RangeInterval(RangeScanner.DUMMY_KEY_START, RangeScanner.DUMMY_KEY_END, false, false),
      ordering), "3.0 is in (-inf, +inf)")

  }

  test("rowInSingleInterval: bound test") {
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row1),
      RangeInterval(row1, row1, false, false), ordering), "1.0 is not in (1.0, 1.0)")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row1),
      RangeInterval(row1, row1, false, true), ordering), "1.0 is not in (1.0, 1.0]")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row1),
      RangeInterval(row1, row1, true, false), ordering), "1.0 is not in [1.0, 1.0)")
    assert(Statistics.rowInSingleInterval(internalRow2unsafeRow(row1),
      RangeInterval(row1, row1, true, true), ordering), "1.0 is in [1.0, 1.0]")
  }

  test("rowInSingleInterval: wrong interval test") {
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(row3, row2, false, false), ordering), "2.0 is not in (3.0, 2.0)")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(row3, row2, false, true), ordering), "2.0 is not in (3.0, 2.0]")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(row3, row2, true, false), ordering), "2.0 is not in [3.0, 2.0)")
    assert(!Statistics.rowInSingleInterval(internalRow2unsafeRow(row2),
      RangeInterval(row3, row2, true, true), ordering), "2.0 is not in [3.0, 2.0]")
  }

  test("rowInIntervalArray") {
    assert(!Statistics.rowInIntervalArray(internalRow2unsafeRow(row1),
      null, ordering), "intervalArray is null")
    assert(Statistics.rowInIntervalArray(internalRow2unsafeRow(InternalRow(1.5)),
      ArrayBuffer(RangeInterval(row1, row2, false, false),
      RangeInterval(row2, row3, false, false)), ordering),
      "1.5 is in (1,2) union (2,3)")
    assert(!Statistics.rowInIntervalArray(internalRow2unsafeRow(InternalRow(-1.0)),
      ArrayBuffer(RangeInterval(row1, row2, false, false),
      RangeInterval(row2, row3, false, false)), ordering),
      "-1.0 is not in (1,2) union (2,3)")
  }

  test("Statistics.writeInternalRow function") {
    // environment setup
    val temp_file = File.createTempFile("spark-5566", ".txt")
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val path = new Path(temp_file.getParent, temp_file.getName)
    val fs = path.getFileSystem(hadoopConf)
    val fileOut = fs.create(path, true)

    // write internalRows out
    val internalRowsToWrite = (0 to 10).map(i => InternalRow(i + 0.0))
    internalRowsToWrite.foreach(Statistics.writeInternalRow(converter, _, fileOut))
    fileOut.close() // writing finished

    // construct expected answer
    // write all content into a ByteArray
    val unsafeRows = internalRowsToWrite.map(row => converter(row).copy())
    val byte_array_buffer = new ByteBufferOutputStream()
    unsafeRows.foreach(row => {
      IndexUtils.writeInt(byte_array_buffer, row.getSizeInBytes)
      byte_array_buffer.write(row.getBytes)
    })
    val expectedAnswer = byte_array_buffer.toByteArray
    byte_array_buffer.close()

    // start reading & checking
    val fin = fs.open(path)
    val length = fs.getContentSummary(path).getLength.toInt
    val readContent = new Array[Byte](length)
    fin.readFully(0, readContent)
    assert(checkByteArray(expectedAnswer, readContent))
  }

  def checkByteArray(expectedAnswer: Array[Byte], actualAnswer: Array[Byte]): Boolean = {
    assert(expectedAnswer.length == actualAnswer.length, "Answer length error")
    if (expectedAnswer.length == 0) true
    else expectedAnswer.zip(actualAnswer).map(tuple => tuple._1 == tuple._2).reduce(_ && _)
  }

  test("getUnsafeRow") {
    val schema = StructType(StructField("test", DoubleType) ::
      StructField("test2", IntegerType) :: Nil)
    val converter = UnsafeProjection.create(schema)
    val ordering = GenerateOrdering.create(schema)
    val unsafeRows = (0 to 100).map(i =>
      converter.apply(InternalRow(i + 0.0, i)).copy())
    val byte_array_stream = new ByteBufferOutputStream()
    unsafeRows.foreach(row => {
      IndexUtils.writeInt(byte_array_stream, row.getSizeInBytes)
      byte_array_stream.write(row.getBytes)
    })
    val byte_array = byte_array_stream.toByteArray
    byte_array_stream.close()

    var offset = 0L
    for (i <- unsafeRows.indices) {
      val size = Platform.getInt(byte_array, Platform.BYTE_ARRAY_OFFSET + offset)
      val unsafeRowFromFile = Statistics.getUnsafeRow(schema.length,
        byte_array, offset, size)
      offset += 4 + size
      assert(ordering.equiv(unsafeRows(i), unsafeRowFromFile))
      assert(checkByteArray(unsafeRows(i).getBytes, unsafeRowFromFile.getBytes))
    }
  }
}
