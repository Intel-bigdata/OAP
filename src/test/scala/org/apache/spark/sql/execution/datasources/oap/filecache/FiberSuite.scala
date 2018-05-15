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


package org.apache.spark.sql.execution.datasources.oap.filecache

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

class FiberSuite extends SharedOapContext with Logging {
  private var file: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    file = Utils.createTempDir()
    file.delete()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(file)
    super.afterAll()
  }

  // Override afterEach because we don't want to check open streams
  override def beforeEach(): Unit = {}
  override def afterEach(): Unit = {}

  test("test reading / writing oap file") {
    val schema = (new StructType)
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", IntegerType)

    val recordCount = 3
    val path = new Path(file.getAbsolutePath, "test1")
    writeData(path, schema, recordCount)
    val split = new FileSplit(
      path, 0, FileSystem.get(configuration).getFileStatus(path).getLen, Array.empty[String])
    assertData(path, schema, Array(0, 1, 2), split, recordCount)
    assertData(path, schema, Array(0, 2, 1), split, recordCount)
    assertData(path, schema, Array(0, 1), split, recordCount)
    assertData(path, schema, Array(2, 1), split, recordCount)
  }

  test("test different data types") {
    val childPath = new Path(file.getAbsolutePath + "test2")
    val recordCount = 100
    // TODO, add more data types when other data types implemented. e.g. ArrayType,
    // CalendarIntervalType, DecimalType, MapType, StructType, TimestampType, etc.
    val schema = (new StructType)
      .add("a", BinaryType)
      .add("b", BooleanType)
      .add("c", ByteType)
      .add("d", DateType)
      .add("e", DoubleType)
      .add("f", FloatType)
      .add("g", IntegerType)
      .add("h", LongType)
      .add("i", ShortType)
      .add("j", StringType)
    writeData(childPath, schema, recordCount)
    val split = new FileSplit(
      childPath, 0,
      FileSystem.get(configuration).getFileStatus(childPath).getLen,
      Array.empty[String])
    assertData(childPath, schema, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), split,
      recordCount)

  }

  test("test data file meta") {
    val previousRowGroupSize = configuration.get(OapFileFormat.ROW_GROUP_SIZE)
    // change default row group size
    configuration.set(OapFileFormat.ROW_GROUP_SIZE, "1024")
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", IntegerType)
    val rowCounts = Array(0, 1023, 1024, 1025)
    val rowCountInLastGroups = Array(0, 1023, 1024, 1)
    val rowGroupCounts = Array(0, 1, 1, 2)
    for (i <- rowCounts.indices) {
      val path = new Path(file.getAbsolutePath, rowCounts(i).toString)
      writeData(path, schema, rowCounts(i))
      val meta = OapDataFile(path.toString, schema, configuration).getDataFileMeta()
      assert(meta.totalRowCount() === rowCounts(i))
      assert(meta.rowCountInLastGroup === rowCountInLastGroups(i))
      assert(meta.rowGroupsMeta.length === rowGroupCounts(i))
      meta.close()
    }
    if (previousRowGroupSize == null) {
      configuration.unset(OapFileFormat.ROW_GROUP_SIZE)
    } else {
      configuration.set(OapFileFormat.ROW_GROUP_SIZE, previousRowGroupSize)
    }
  }

  test("test oap row group configuration") {
    val previousRowGroupSize = configuration.get(OapFileFormat.ROW_GROUP_SIZE)
    // change default row group size
    configuration.set(OapFileFormat.ROW_GROUP_SIZE, "12345")
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", IntegerType)

    val path = new Path(file.getAbsolutePath, 10.toString)
    writeData(path, schema, 10)

    val meta = OapDataFile(path.toString, schema, configuration).getDataFileMeta()
    assert(meta.totalRowCount() === 10)
    assert(meta.rowCountInEachGroup === 12345)
    assert(meta.rowCountInLastGroup === 10)
    assert(meta.rowGroupsMeta.length === 1)
    meta.close()
    // set back to default value
    if (previousRowGroupSize == null) {
      configuration.unset(OapFileFormat.ROW_GROUP_SIZE)
    } else {
      configuration.set(OapFileFormat.ROW_GROUP_SIZE, previousRowGroupSize)
    }
  }

  // a simple algorithm to check if it's should be null
  private def shouldBeNull(rowId: Int, fieldId: Int): Boolean = {
    rowId % (fieldId + 3) == 0
  }

  def writeData(
      path: Path,
      schema: StructType, count: Int): Unit = {
    val out = FileSystem.get(configuration).create(path, true)
    val writer = new OapDataWriter(false, out, schema, configuration)
    val row = new GenericInternalRow(schema.fields.length)
    for(i <- 0 until count) {
      schema.fields.zipWithIndex.foreach { entry =>
        if (shouldBeNull(i, entry._2)) {
          // let's make some nulls
          row.setNullAt(entry._2)
        } else {
          entry match {
            case (StructField(_, BinaryType, true, _), idx) =>
              row.update(idx, Array[Byte](i.toByte, i.toByte))
            case (StructField(_, BooleanType, true, _), idx) =>
              val bool = if (i % 2 == 0) true else false
              row.setBoolean(idx, bool)
            case (StructField(_, ByteType, true, _), idx) =>
              row.setByte(idx, i.toByte)
            case (StructField(_, DateType, true, _), idx) =>
              row.setInt(idx, i)
            case (StructField(_, DoubleType, true, _), idx) =>
              row.setDouble(idx, i.toDouble / 3)
            case (StructField(_, FloatType, true, _), idx) =>
              row.setFloat(idx, i.toFloat / 3)
            case (StructField(_, IntegerType, true, _), idx) =>
              row.setInt(idx, i)
            case (StructField(_, LongType, true, _), idx) =>
              row.setLong(idx, i.toLong * 41)
            case (StructField(_, ShortType, true, _), idx) =>
              row.setShort(idx, i.toShort)
            case (StructField(name, StringType, true, _), idx) =>
              row.update(idx, UTF8String.fromString(s"$name Row $i"))
            case _ => throw new NotImplementedError("TODO")
          }
        }
      }
      writer.write(row)
    }
    writer.close()
  }

  def assertData(
      path: Path,
      schema: StructType,
      requiredIds: Array[Int],
      split: FileSplit,
      count: Int): Unit = {
    val m = DataSourceMeta.newBuilder().
      withNewSchema(schema).
      withNewDataReaderClassName(OapFileFormat.OAP_DATA_FILE_CLASSNAME).build()
    val reader = new OapDataReader(path, m, None, requiredIds)
    val it = reader.initialize(configuration)

    var idx = 0
    while (it.hasNext) {
      val row = it.next()
      assert(row.numFields === requiredIds.length)
      requiredIds.zipWithIndex.foreach { case (fid, outputId) =>
        if (shouldBeNull(idx, fid)) {
          assert(row.isNullAt(outputId))
        } else {
          schema(fid) match {
            case StructField(_, BinaryType, true, _) =>
              assert(Array[Byte](idx.toByte, idx.toByte) === row.getBinary(outputId))
            case StructField(_, BooleanType, true, _) =>
              val bool = if (idx % 2 == 0) true else false
              assert(bool === row.getBoolean(outputId))
            case StructField(_, ByteType, true, _) =>
              assert(idx.toByte === row.getByte(outputId))
            case StructField(_, DateType, true, _) =>
              assert(idx === row.get(outputId, DateType))
            case StructField(_, DoubleType, true, _) =>
              assert(idx.toDouble / 3 === row.getDouble(outputId))
            case StructField(_, FloatType, true, _) =>
              assert(idx.toFloat / 3 === row.getFloat(outputId))
            case StructField(_, IntegerType, true, _) =>
              assert(idx === row.getInt(outputId))
            case StructField(_, LongType, true, _) =>
              assert(idx.toLong * 41 === row.getLong(outputId))
            case StructField(_, ShortType, true, _) =>
              assert(idx.toShort === row.getShort(outputId))
            case StructField(name, StringType, true, _) =>
              assert(s"$name Row $idx" === row.getString(outputId))
            case _ => throw new NotImplementedError("TODO")
          }
        }
      }
      idx += 1
    }
    assert(idx === count)
  }
}
