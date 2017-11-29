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
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ByteBufferOutputStream, Utils}

class BTreeRecordReaderWriterSuite extends SparkFunSuite {

  private lazy val random = new Random(0)
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
    val writer = BTreeIndexRecordWriter(configuration, fileWriter, schema)
    records.map(InternalRow(_)).foreach(writer.write(null, _))
    writer.close(null)
    fileWriter
  }

  test("check read/write nodes") {
    // answer stores sorted unique key list, and the start pos in (sorted) row id list
    val answer = fileWriter.nodes.flatMap { buf =>
      val node = BTreeIndexRecordReader.BTreeNodeData(FiberCache(buf))
      (0 until node.getKeyCount).map(i => (node.getRowIdPos(i), node.getKey(i, schema).getInt(0)))
    }
    assert(answer === records.sorted.distinct.map(v => (records.sorted.indexOf(v), v)))
  }

  test("check read/write rowIdList") {
    val rowIdList = BTreeIndexRecordReader.BTreeRowIdList(FiberCache(fileWriter.rowIdList))
    assert(records.sorted === records.indices.map(rowIdList.getRowId).map(records(_)))
  }

  test("check read/write footer") {
    val footer = BTreeIndexRecordReader.BTreeFooter(FiberCache(fileWriter.footer))
    val nodeCount = footer.getNodesCount
    assert(footer.getRecordCount === records.size)
    assert(nodeCount === fileWriter.nodes.size)

    val nodeSizeSeq = (0 until nodeCount).map(footer.getNodeSize)
    val nodeOffsetSeq = (0 until nodeCount).map(footer.getNodeOffset)
    assert(nodeSizeSeq === fileWriter.nodes.map(_.length))
    assert(nodeOffsetSeq === nodeSizeSeq.scanLeft(0)(_ + _).dropRight(1))

    val keyOffsetSeq = (0 until nodeCount).map ( i =>
      BTreeIndexRecordReader.BTreeNodeData(FiberCache(fileWriter.nodes(i))).getKeyCount
    ).scanLeft(0)(_ + _)
    val uniqueValues = records.sorted.distinct
    (0 until nodeCount).foreach { i =>
      assert(uniqueValues(keyOffsetSeq(i)) === footer.getMinValue(i, schema).getInt(0))
      assert(uniqueValues(keyOffsetSeq(i + 1) - 1) === footer.getMaxValue(i, schema).getInt(0))
    }
  }
}
