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

import org.apache.hadoop.fs.Path
import org.junit.Assert._

import org.apache.spark.internal.Logging
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.unsafe.Platform

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
}
