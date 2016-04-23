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
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptContext, TaskAttemptID, TaskID}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkFunSuite}
import org.scalatest.BeforeAndAfterAll

class FiberSuite extends SparkFunSuite with Logging with BeforeAndAfterAll {
  private var file: File = null
  val attemptContext: TaskAttemptContext = new TaskAttemptContextImpl(
    new Configuration(),
    new TaskAttemptID(new TaskID(new JobID(), true, 0), 0))
  val ctx: Configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(attemptContext)
  val schema = new StructType().add("a", IntegerType).add("b", StringType).add("c", IntegerType)
  var path: Path = _

  override def beforeAll(): Unit = {
    file = Utils.createTempDir()
    file.delete()
    path = new Path(StringUtils.unEscapeString(file.toURI.toString))
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(file)
  }

  test("reading / writing spinach file") {
    val recordCount = 3
    writeData(ctx, path, schema, recordCount, attemptContext)
    val split = new FileSplit(
      path, 0, FileSystem.get(ctx).getFileStatus(path).getLen(), Array.empty[String])
    assertData(path, schema, Array(0, 1, 2), split, attemptContext, recordCount)
    assertData(path, schema, Array(0, 2, 1), split, attemptContext, recordCount)
    assertData(path, schema, Array(0, 1), split, attemptContext, recordCount)
    assertData(path, schema, Array(2, 1), split, attemptContext, recordCount)
  }

  // a simple algorithm to check if it's should be null
  private def shouldBeNull(rowId: Int, fieldId: Int): Boolean = {
    rowId % (fieldId + 3) == 0
  }

  def writeData(
      ctx: Configuration, path: Path,
      schema: StructType, count: Int,
      attemptContext: TaskAttemptContext): Unit = {
    val out = FileSystem.get(ctx).create(path, true)
    val writer = new SpinachDataWriter2(false, out, schema)
    val row = new GenericMutableRow(schema.fields.length)
    for(i <- 0 until count) {
      schema.fields.zipWithIndex.foreach { entry =>
        if (shouldBeNull(i, entry._2)) {
          // let's make some nulls
          row.setNullAt(entry._2)
        } else {
          entry match {
            case (StructField(name, IntegerType, true, _), idx) =>
              row.setInt(idx, i)
            case (StructField(name, StringType, true, _), idx) =>
              row.update(idx, UTF8String.fromString(s"$name Row $i"))
            case _ => throw new NotImplementedError("TODO")
          }
        }
      }
      writer.write(null, row)
    }
    writer.close(attemptContext)
  }

  def assertData(
      path: Path,
      schema: StructType,
      requiredIds: Array[Int],
      split: FileSplit,
      attemptContext: TaskAttemptContext,
      count: Int): Unit = {
    val reader = new SpinachDataReader2(path, schema, Array(0, 1, 2))
    reader.initialize(split, attemptContext)

    var idx = 0
    while (reader.nextKeyValue()) {
      val row = reader.getCurrentValue
      assert(row.numFields === requiredIds.length)
      requiredIds.foreach { fid =>
        if (shouldBeNull(idx, fid)) {
          assert(row.isNullAt(fid))
        } else {
          schema(fid) match {
            case StructField(name, IntegerType, true, _) =>
              assert(idx === row.getInt(fid))
            case StructField(name, StringType, true, _) =>
              assert(s"$name Row $idx" === row.getString(fid))
            case _ => throw new NotImplementedError("TODO")
          }
        }
      }
      idx += 1
    }
    assert(idx === count)
  }
}
