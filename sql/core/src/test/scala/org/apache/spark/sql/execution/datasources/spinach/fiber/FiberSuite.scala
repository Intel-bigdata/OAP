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

package org.apache.spark.sql.execution.datasources.spinach.fiber

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.StringUtils
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfterAll

class FiberSuite extends SparkFunSuite with Logging with BeforeAndAfterAll {
  private var file: File = null

  override def beforeAll(): Unit = {
    file = Utils.createTempDir()
    file.delete()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(file)
  }

  test("reading / writing spinach file") {
    val attemptContext = new TaskAttemptContextImpl(
      new Configuration(),
      new TaskAttemptID(new TaskID(new JobID(), true, 0), 0))

    val ctx = SparkHadoopUtil.get.getConfigurationFromJobContext(attemptContext)
    val path = new Path(StringUtils.unEscapeString(file.toURI.toString))
    val out = FileSystem.get(ctx).create(path, true)

    val schema = new StructType().add("a", IntegerType).add("b", StringType).add("c", IntegerType)
    val writer = new FiberDataWriter(false, out, schema)
    val row = new GenericMutableRow(3)
    for(i <- 0 until 1026) {
      row.setInt(0, i)
      row.update(1, UTF8String.fromString(s"This is Row $i"))
      row.setInt(2, i + 2000)
      writer.write(null, row)
    }
    writer.close(null)

    val reader = new FiberDataReader(path, schema, Array(0, 1, 2))
    val split = new FileSplit(
      path, 0, FileSystem.get(ctx).getFileStatus(path).getLen(), Array.empty[String])
    reader.initialize(split, attemptContext)
    var idx = 0
    while (reader.nextKeyValue()) {
      val row = reader.getCurrentValue
      assert(idx === row.getInt(0))
      assert(idx + 2000 === row.getInt(2))
      assert(s"This is Row $idx" === row.getString(1))
      idx += 1
    }
  }
}
