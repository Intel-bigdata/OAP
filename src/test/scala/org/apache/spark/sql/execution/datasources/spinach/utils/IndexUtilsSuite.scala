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

package org.apache.spark.sql.execution.datasources.spinach.utils

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.junit.Assert._

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.spinach.index.{IndexOutputWriter, IndexUtils}
import org.apache.spark.unsafe.Platform

class TestIndexOutputWriter extends IndexOutputWriter(bucketId = None, context = null) {
  val buf = new ByteArrayOutputStream(8)
  override protected lazy val writer: RecordWriter[Void, Any] =
    new RecordWriter[Void, Any] {
      override def close(context: TaskAttemptContext) = buf.close()
      override def write(key: Void, value: Any) = value match {
        case bytes: Array[Byte] => buf.write(bytes)
        case i: Int => buf.write(i) // this will only write a byte
      }
    }
}

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

  test("write int to IndexOutputWriter") {
    val out = new TestIndexOutputWriter
    IndexUtils.writeInt(out, -19)
    IndexUtils.writeInt(out, 4321)
    out.close()
    val bytes = out.buf.toByteArray
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET) == -19)
    assert(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + 4) == 4321)
  }

  test("write long to IndexOutputWriter") {
    val out = new TestIndexOutputWriter
    IndexUtils.writeLong(out, -19)
    IndexUtils.writeLong(out, 4321)
    IndexUtils.writeLong(out, 43210912381723L)
    IndexUtils.writeLong(out, -99128917321912L)
    out.close()
    val bytes = out.buf.toByteArray
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
}
