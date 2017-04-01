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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.execution.datasources.spinach.io.DataReaderWriter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils


class DataSuite extends SparkFunSuite with Logging with BeforeAndAfterAll {
  private var file: File = null
  val conf: Configuration = new Configuration()

  override def beforeAll(): Unit = {
    System.setProperty("spinach.rowgroup.size", "1024")
    file = Utils.createTempDir()
    // file.delete()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(file)
  }

  test("test reading / writing spinach file") {
    val schema = (new StructType)
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", IntegerType)

    val recordCount = 3
    val path = new Path(file.getAbsolutePath, "test1")
    writeData(path, schema, recordCount)
    val split = new FileSplit(
      path, 0, FileSystem.get(conf).getFileStatus(path).getLen(), Array.empty[String])
    assertData(path, schema, split, recordCount)
    assertData(path, schema, split, recordCount)
    assertData(path, schema, split, recordCount)
    assertData(path, schema, split, recordCount)
  }

  test("test different data types") {
    val childPath = new Path(file.getAbsolutePath + "test2")
    val recordCount = 100
    // TODO, add more data types when other data types implemented. e.g. ArrayType,
    // CalendarIntervalType, DecimalType, MapType, StructType, TimestampType, etc.
    val schema = (new StructType)
      .add("b", BooleanType)
      .add("c", ByteType)
      .add("d", DateType)
      .add("e", DoubleType)
      .add("f", FloatType)
      .add("g", IntegerType)
      .add("h", LongType)
      .add("i", ShortType)
      .add("j", StringType)
      .add("k", TimestampType)
    writeData(childPath, schema, recordCount)
    val split = new FileSplit(
      childPath, 0, FileSystem.get(conf).getFileStatus(childPath).getLen(), Array.empty[String])
    assertData(childPath, schema, split, recordCount)
  }

  // a simple algorithm to check if it's should be null
  private def shouldBeNull(rowId: Int, fieldId: Int): Boolean = {
    rowId % (fieldId + 3) == 0
  }

  def writeData(
      path: Path,
      schema: StructType, count: Int): Unit = {
    val out = FileSystem.get(conf).create(path, true)
    val writers = DataReaderWriter.initialDataReaderWriterFromSchema(schema)
    val row = new GenericMutableRow(schema.fields.length)
    for(i <- 0 until count) {
      schema.fields.zipWithIndex.foreach { entry =>
        val idx = entry._2
        if (shouldBeNull(i, idx)) {
          // let's make some nulls
          row.setNullAt(idx)
        } else {
          entry._1 match {
            case StructField(name, BooleanType, true, _) =>
              val bool = if (i % 2 == 0) true else false
              row.setBoolean(idx, bool)
            case StructField(name, ByteType, true, _) =>
              row.setByte(idx, i.toByte)
            case StructField(name, DateType, true, _) =>
              row.setInt(idx, i)
            case StructField(name, DoubleType, true, _) =>
              row.setDouble(idx, i.toDouble / 3)
            case StructField(name, FloatType, true, _) =>
              row.setFloat(idx, i.toFloat / 3)
            case StructField(name, IntegerType, true, _) =>
              row.setInt(idx, i)
            case StructField(name, LongType, true, _) =>
              row.setLong(idx, i.toLong * 41)
            case StructField(name, ShortType, true, _) =>
              row.setShort(idx, i.toShort)
            case StructField(name, StringType, true, _) =>
              row.update(idx, UTF8String.fromString(s"$name Row $i"))
            case StructField(name, TimestampType, true, _) =>
              row.setLong(idx, i.toLong * 11)
            case _ => throw new NotImplementedError("TODO")
          }
        }
        writers(idx).write(out, row)
      }
    }
    out.close()
  }

  def assertData(
      path: Path,
      schema: StructType,
      split: FileSplit,
      count: Int): Unit = {
    val in = FileSystem.get(conf).open(path)
    val readers = DataReaderWriter.initialDataReaderWriterFromSchema(schema)
    val row = new GenericMutableRow(schema.fields.length)

    var i = 0
    while(i < count) {
      schema.zipWithIndex.foreach { entry =>
        val idx = entry._2
        readers(idx).read(in, row)
        if (shouldBeNull(i, idx)) {
          assert(row.isNullAt(idx))
        } else {
          entry._1 match {
            case StructField(name, BooleanType, true, _) =>
              val bool = if (i % 2 == 0) true else false
              assert(bool == row.getBoolean(idx))
            case StructField(name, ByteType, true, _) =>
              assert(i.toByte == row.getByte(idx))
            case StructField(name, DateType, true, _) =>
              assert(i == row.get(idx, DateType))
            case StructField(name, DoubleType, true, _) =>
              assert(i.toDouble / 3 == row.getDouble(idx))
            case StructField(name, FloatType, true, _) =>
              assert(i.toFloat / 3 == row.getFloat(idx))
            case StructField(name, IntegerType, true, _) =>
              assert(i == row.getInt(idx))
            case StructField(name, LongType, true, _) =>
              assert(i.toLong * 41 == row.getLong(idx))
            case StructField(name, ShortType, true, _) =>
              assert(i.toShort == row.getShort(idx))
            case StructField(name, StringType, true, _) =>
              assert(s"$name Row $i" == row.getString(idx))
            case StructField(name, TimestampType, true, _) =>
              assert(i.toLong * 11 == row.getLong(idx))
            case _ => throw new NotImplementedError("TODO")
          }
        }
      }
      i += 1
    }
    assert(i == count)
    in.close()
  }
}
