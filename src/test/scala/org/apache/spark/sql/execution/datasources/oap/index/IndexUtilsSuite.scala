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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
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
    IndexUtils.writeHead(out, IndexFile.VERSION_NUM)
    val bytes = buf.toByteArray
    assert(Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + 6) ==
      (IndexFile.VERSION_NUM >> 8).toByte)
    assert(Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + 7) ==
      (IndexFile.VERSION_NUM & 0xFF).toByte)
  }

  test("binary search") {
    def compare(x: InternalRow, y: InternalRow): Int = {
      val xVal = x.getInt(0)
      val yVal = y.getInt(0)
      xVal - yVal
    }
    assert(IndexUtils.binarySearch(
      0, 10, i => InternalRow(i + 5), InternalRow(7), compare) == (2, true))
    assert(IndexUtils.binarySearch(
      0, 10, i => InternalRow(i + 5), InternalRow(5), compare) == (0, true))
    assert(IndexUtils.binarySearch(
      0, 10, i => InternalRow(i + 5), InternalRow(4), compare) == (0, false))
    assert(IndexUtils.binarySearch(
      0, 10, i => InternalRow(i + 5), InternalRow(15), compare) == (10, false))
    assert(IndexUtils.binarySearch(
      0, 10, i => InternalRow(i + 5), InternalRow(14), compare) == (9, true))
    assert(IndexUtils.binarySearch(
      0, 0, i => InternalRow(i + 5), InternalRow(5), compare) == (0, false))

    assert(IndexUtils.binarySearchForStart(
      0, 10, i => InternalRow(i + 4), InternalRow(7), compare) == (3, true))
    assert(IndexUtils.binarySearchForStart(
      0, 10, i => InternalRow(i / 3), InternalRow(2), compare) == (6, true))
    assert(IndexUtils.binarySearchForStart(
      0, 10, i => InternalRow(i / 3), InternalRow(0), compare) == (0, true))
    assert(IndexUtils.binarySearchForStart(
      0, 0, i => InternalRow(i / 3), InternalRow(0), compare) == (0, false))
    assert(IndexUtils.binarySearchForStart(
      0, 1, i => InternalRow(i / 3), InternalRow(0), compare) == (0, true))
    assert(IndexUtils.binarySearchForStart(
      0, 1, i => InternalRow(i / 3), InternalRow(1), compare) == (1, false))
    assert(IndexUtils.binarySearchForStart(
      0, 1, i => InternalRow(i / 3), InternalRow(-1), compare) == (0, false))
    assert(IndexUtils.binarySearchForStart(
      0, 10, i => InternalRow(i / 3), InternalRow(-11), compare) == (0, false))
    assert(IndexUtils.binarySearchForStart(
      0, 10, i => InternalRow(i / 3), InternalRow(11), compare) == (10, false))
    assert(IndexUtils.binarySearchForStart(
      0, 10, i => InternalRow(i * 2), InternalRow(3), compare) == (2, false))

    assert(IndexUtils.binarySearchForEnd(
      0, 10, i => InternalRow(i + 4), InternalRow(13), compare) == (9, true))
    assert(IndexUtils.binarySearchForEnd(
      0, 10, i => InternalRow(i / 3), InternalRow(2), compare) == (8, true))
    assert(IndexUtils.binarySearchForEnd(
      0, 10, i => InternalRow(i / 3), InternalRow(3), compare) == (9, true))
    assert(IndexUtils.binarySearchForEnd(
      0, 10, i => InternalRow(i / 3), InternalRow(5), compare) == (10, false))
    assert(IndexUtils.binarySearchForEnd(
      0, 10, i => InternalRow(i / 3), InternalRow(0), compare) == (2, true))
    assert(IndexUtils.binarySearchForEnd(
      0, 10, i => InternalRow(i / 3), InternalRow(-1), compare) == (-1, false))
    assert(IndexUtils.binarySearchForEnd(
      0, 0, i => InternalRow(i / 3), InternalRow(0), compare) == (-1, false))
    assert(IndexUtils.binarySearchForEnd(
      0, 1, i => InternalRow(i / 3), InternalRow(0), compare) == (0, true))
    assert(IndexUtils.binarySearchForEnd(
      0, 10, i => InternalRow(i * 2), InternalRow(3), compare) == (2, false))
  }

  test("index version test") {
    // Simple test to check IndexVersion conversion. Should never fail.
    IndexVersion.values.foreach { v =>
      assert(IndexVersion.fromId(v.id) == v)
    }
    IndexVersion.values.foreach { v =>
      assert(IndexVersion.fromString(v.toString) == v)
    }
    // Check invalid id/name conversion
    val exception1 = intercept[OapException] {
      IndexVersion.fromId(-1)
    }
    assert(exception1.getMessage == "Unsupported index version. id: -1")
    val exception2 = intercept[OapException] {
      IndexVersion.fromString("v-1")
    }
    assert(exception2.getMessage == "Unsupported index version. name: v-1")
  }
}
