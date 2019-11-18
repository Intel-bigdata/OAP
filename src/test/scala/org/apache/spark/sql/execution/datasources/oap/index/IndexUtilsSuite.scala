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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, OapException, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.Platform

class IndexUtilsSuite extends SparkFunSuite with SharedOapContext with Logging {
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

  test("getIndexFilePath: get index file path with the default configuration") {
    // In the default configuration of OapConf.OAP_INDEX_DIRECTORY
    val option = Map(
      OapConf.OAP_INDEX_DIRECTORY.key -> spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key))
    val conf = spark.sessionState.newHadoopConfWithOptions(option)
    // In default configuration, the index file path is the data file path.
    assertEquals("/path/to/.t1.ABC.index1.index",
      IndexUtils.getIndexFilePath(
        conf, new Path("/path/to/t1.data"), "index1", "ABC").toString)
    assertEquals("/.t1.1F23.index1.index",
      IndexUtils.getIndexFilePath(
        conf, new Path("/t1.data"), "index1", "1F23").toString)
    assertEquals("/path/to/.t1.0.index1.index",
      IndexUtils.getIndexFilePath(
        conf, new Path("/path/to/t1.parquet"), "index1", "0").toString)
    assertEquals("/path/to/.t1.F91.index1.index",
      IndexUtils.getIndexFilePath(
        conf, new Path("/path/to/t1"), "index1", "F91").toString)
  }

  test("getIndexFilePath: get index file path with specific configuration of blank String") {
    // set the configuration of OapConf.OAP_INDEX_DIRECTORY to blank String
    withSQLConf(OapConf.OAP_INDEX_DIRECTORY.key -> "   ") {
      val option = Map(
        OapConf.OAP_INDEX_DIRECTORY.key -> spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key))
      val conf = spark.sessionState.newHadoopConfWithOptions(option)
      // because the indexDirectory is " ", so the index file path is the data file path
      assertEquals("/path/to/.t1.ABC.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/path/to/t1.data"), "index1", "ABC").toString)
      assertEquals("/.t1.1F23.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/t1.data"), "index1", "1F23").toString)
      assertEquals("/path/to/.t1.0.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/path/to/t1.parquet"), "index1", "0").toString)
      assertEquals("/path/to/.t1.F91.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/path/to/t1"), "index1", "F91").toString)
    }
  }

  test("getIndexFilePath: get index file path with specific configuration") {
    // set the configuration of OapConf.OAP_INDEX_DIRECTORY
    withSQLConf(OapConf.OAP_INDEX_DIRECTORY.key -> "/tmp") {
      val indexDirectory = spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key)
      val option = Map(
        OapConf.OAP_INDEX_DIRECTORY.key -> spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key))
      val conf = spark.sessionState.newHadoopConfWithOptions(option)
      // because the indexDirectory is "/tmp", so the index file path is "/tmp" +data file path
      assertEquals(s"$indexDirectory/path/to/.t1.ABC.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/path/to/t1.data"), "index1", "ABC").toString)
      assertEquals(s"$indexDirectory/.t1.1F23.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/t1.data"), "index1", "1F23").toString)
      assertEquals(s"$indexDirectory/path/to/.t1.0.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/path/to/t1.parquet"), "index1", "0").toString)
      assertEquals(s"$indexDirectory/path/to/.t1.F91.index1.index",
        IndexUtils.getIndexFilePath(
          conf, new Path("/path/to/t1"), "index1", "F91").toString)
    }
  }

  test("generateTempIndexFilePath: generating temp index file path with default configuration") {
    // test the default configuration of OapConf.OAP_INDEX_DIRECTORY
    val option = Map(
      OapConf.OAP_INDEX_DIRECTORY.key -> spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key))
    val conf = spark.sessionState.newHadoopConfWithOptions(option)
    // In default configuration, the index file path is the data file path.
    assertEquals("/path/to/_temp/0/.t1.ABC.index1.index",
      IndexUtils.generateTempIndexFilePath(conf,
        "/path/to/t1.data",
        new Path("/path/to"),
        "/path/to/_temp/0/.index",
        ".ABC.index1.index").toString)
    assertEquals("hdfs:/path/to/_temp/1/a=3/b=4/.t1.ABC.index1.index",
      IndexUtils.generateTempIndexFilePath(conf,
        "hdfs:/path/to/a=3/b=4/t1.data",
        new Path("/path/to"),
        "/path/to/_temp/1/.index",
        ".ABC.index1.index").toString)
    assertEquals("hdfs://remote:8020/path/to/_temp/2/x=1/.t1.ABC.index1.index",
      IndexUtils.generateTempIndexFilePath(conf,
        "hdfs://remote:8020/path/to/x=1/t1.data",
        new Path("/path/to/"),
        "/path/to/_temp/2/.index",
        ".ABC.index1.index").toString)

  }

  test("generateTempIndexFilePath: generating temp index file path with" +
    " specific configuration of blank String") {
    // set the configuration of OapConf.OAP_INDEX_DIRECTORY to blank String
    withSQLConf(OapConf.OAP_INDEX_DIRECTORY.key -> "   ") {
      val option = Map(
        OapConf.OAP_INDEX_DIRECTORY.key -> spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key))
      val conf = spark.sessionState.newHadoopConfWithOptions(option)
      // because the indexDirectory is " ", so the index file path is the data file path
      assertEquals("/path/to/_temp/0/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "/path/to/t1.data",
          new Path("/path/to"),
          "/path/to/_temp/0/.index",
          ".ABC.index1.index").toString)
      assertEquals("hdfs:/path/to/_temp/1/a=3/b=4/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "hdfs:/path/to/a=3/b=4/t1.data",
          new Path("/path/to"),
          "/path/to/_temp/1/.index",
          ".ABC.index1.index").toString)
      assertEquals("hdfs://remote:8020/path/to/_temp/2/x=1/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "hdfs://remote:8020/path/to/x=1/t1.data",
          new Path("/path/to/"),
          "/path/to/_temp/2/.index",
          ".ABC.index1.index").toString)
    }
  }


  test("generateTempIndexFilePath: generating temp index file path with specific configuration") {
    // set the configuration of OapConf.OAP_INDEX_DIRECTORY
    withSQLConf(OapConf.OAP_INDEX_DIRECTORY.key -> "/tmp") {
      val indexDirectory = spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key)
      val option = Map(
        OapConf.OAP_INDEX_DIRECTORY.key -> spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key))
      val conf = spark.sessionState.newHadoopConfWithOptions(option)
      // because the indexDirectory is "/tmp", so the index file path is "/tmp" +data file path
      assertEquals(s"$indexDirectory/path/to/_temp/0/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "/path/to/t1.data",
          new Path(s"$indexDirectory/path/to"),
          s"$indexDirectory/path/to/_temp/0/.index",
          ".ABC.index1.index").toString)
      assertEquals(s"$indexDirectory/path/to/_temp/1/a=3/b=4/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "hdfs:/path/to/a=3/b=4/t1.data",
          new Path(s"$indexDirectory/path/to"),
          s"$indexDirectory/path/to/_temp/1/.index",
          ".ABC.index1.index").toString)
      assertEquals(s"$indexDirectory/path/to/_temp/2/x=1/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "hdfs://remote:8020/path/to/x=1/t1.data",
          new Path(s"$indexDirectory/path/to/"),
          s"$indexDirectory/path/to/_temp/2/.index",
          ".ABC.index1.index").toString)
    }
  }

  test("generateTempIndexFilePath: generating temp index file path with specific" +
    " configuration with schema") {
    // set the configuration of OapConf.OAP_INDEX_DIRECTORY with schema
    withSQLConf(OapConf.OAP_INDEX_DIRECTORY.key -> "file:/tmp") {
      val indexDirectory = spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key)
      val option = Map(
        OapConf.OAP_INDEX_DIRECTORY.key -> spark.conf.get(OapConf.OAP_INDEX_DIRECTORY.key))
      val conf = spark.sessionState.newHadoopConfWithOptions(option)
      // because the indexDirectory is "file:/tmp",
      // so the index file path is "file:/tmp" +data file path
      assertEquals(s"$indexDirectory/path/to/_temp/0/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "/path/to/t1.data",
          new Path(s"$indexDirectory/path/to"),
          s"$indexDirectory/path/to/_temp/0/.index",
          ".ABC.index1.index").toString)
      assertEquals(s"$indexDirectory/path/to/_temp/1/a=3/b=4/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "hdfs:/path/to/a=3/b=4/t1.data",
          new Path(s"$indexDirectory/path/to"),
          s"$indexDirectory/path/to/_temp/1/.index",
          ".ABC.index1.index").toString)
      assertEquals(s"$indexDirectory/path/to/_temp/2/x=1/.t1.ABC.index1.index",
        IndexUtils.generateTempIndexFilePath(conf,
          "hdfs://remote:8020/path/to/x=1/t1.data",
          new Path(s"$indexDirectory/path/to/"),
          s"$indexDirectory/path/to/_temp/2/.index",
          ".ABC.index1.index").toString)
    }
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

  test("test rootPaths empty") {
    val fileIndex = new InMemoryFileIndex(spark, Seq.empty, Map.empty, None)
    intercept[AssertionError] {
      IndexUtils.getOutputPathBasedOnConf(fileIndex, spark.conf)
    }
  }

  test("test rootPaths length eq 1 no partitioned") {
    val tablePath = new Path("/table")
    val rootPaths = Seq(tablePath)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, None)
    val ret = IndexUtils.getOutputPathBasedOnConf(fileIndex, spark.conf)
    assert(ret.equals(tablePath))
  }

  test("test rootPaths length eq 1 partitioned") {
    val tablePath = new Path("/table")
    val partitionSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StringType))
    val rootPaths = Seq(tablePath)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, Some(partitionSchema))
    val ret = IndexUtils.getOutputPathBasedOnConf(fileIndex, spark.conf)
    assert(ret.equals(tablePath))
  }

  // with spark2.4.1, we can not get the partition path when the path does not contain files.
  /* test("test rootPaths length more than 1") {
    val part1 = new Path("/table/a=1/b=1")
    val part2 = new Path("/table/a=1/b=2")
    val tablePath = new Path("/table")
    val partitionSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StringType))
    val rootPaths = Seq(part1, part2)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, Some(partitionSchema))
    val ret = IndexUtils.getOutputPathBasedOnConf(fileIndex, spark.conf)
    assert(ret.equals(tablePath))
  } */

  test("test rootPaths length eq 1 but partitioned") {
    val part1 = new Path("/table/a=1/b=1")
    val partitionSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StringType))
    val rootPaths = Seq(part1)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, Some(partitionSchema))
    val ret = IndexUtils.getOutputPathBasedOnConf(fileIndex, spark.conf)
    assert(ret.equals(part1))
  }

  test("test build partitions filter") {
    val rows = Array.fill[GenericInternalRow](3)(new GenericInternalRow(3))
    val values = Seq(Array(1, "a", 2), Array(1, "b", 1), Array(2, "c", 1))
    val internalRows = rows.zip(values).map { case (row, value) =>
      value.indices.foreach(i => row.update(i, value(i)))
      row
    }
    val partitionSchema = new StructType()
      .add(StructField("col_a", IntegerType))
      .add(StructField("col_b", StringType))
      .add(StructField("col_c", IntegerType))
    val partitions = internalRows.map(PartitionDirectory(_, Nil))
    val filters = IndexUtils.buildPartitionsFilter(partitions, partitionSchema)
    val expression = spark.sessionState.sqlParser.parseExpression(filters)

    val expectStr = "(col_a = '1' and col_b = 'a' and col_c = '2') " +
      "or (col_a = '1' and col_b = 'b' and col_c = '1') " +
      "or (col_a = '2' and col_b = 'c' and col_c = '1')"
    val expectExpr = spark.sessionState.sqlParser.parseExpression(expectStr)

    assert(expression.semanticEquals(expectExpr))
  }
}
