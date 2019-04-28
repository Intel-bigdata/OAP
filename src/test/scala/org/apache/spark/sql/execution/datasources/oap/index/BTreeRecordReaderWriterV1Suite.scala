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


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.Properties

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SecurityManager, TaskContext, TaskContextImpl}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.oap.adapter.TaskContextImplAdapter
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types._

class BTreeRecordReaderWriterV1Suite extends SharedOapContext {

  private lazy val random = new Random(0)
  /**
   * This class is used to inject into BTreeRecordWriter and write index data to memory
   */
  private class TestIndexFileWriter() extends IndexFileWriter {

    override protected val os: ByteArrayOutputStream = new ByteArrayOutputStream()

    def toByteArray: Array[Byte] = {
      os.toByteArray
    }

    override def getName: String = "Memory File Writer"

    // Only for create row id temp file use.
    override def tempRowIdWriter: IndexFileWriter = new TestIndexFileWriter()

    override def writeRowId(tempWriter: IndexFileWriter): Unit = {
      val tempBytes = tempWriter.asInstanceOf[TestIndexFileWriter].toByteArray
      os.write(tempBytes)
    }
  }

  /**
   * This class is used to inject into BTreeRecordReader and read index data from memory
   */
  private class TestIndexFileReader(bytes: Array[Byte]) extends IndexFileReader {
    override protected val is: InputStream = new ByteArrayInputStream(bytes)

    override def readFiberCache(position: Long, length: Int): FiberCache = {
      // Note: Use DataFiberCache instead of IndexFiberCache to reuse current interface
      // DataFiberCache and IndexFiberCache are identical actually.
      OapRuntime.getOrCreate.memoryManager.toDataFiberCache(read(position, length))
    }

    override def read(position: Long, length: Int): Array[Byte] = {
      val bytes = new Array[Byte](length)
      is.reset()
      is.skip(position)
      is.read(bytes)
      bytes
    }

    override def readFully(position: Long, buf: Array[Byte]): Unit = {
      is.reset()
      is.skip(position)
      is.read(buf)
    }

    override def getLen: Long = bytes.length

    override def getName: String = "Memory File Reader: " + bytes.hashCode()
  }

  // Only test simple Int type since read/write based on schema can cover data type test
  private val schema = StructType(StructField("col", IntegerType) :: Nil)
  private val nonNullKeyRecords = (0 until 1000).map(_ => random.nextInt(1000 / 2))
  private val nullKeyRecords = (1 to 5).map(_ => null)

  test("write then read index") {
    val configuration = new Configuration()
    // Create writer
    val fileWriter = new TestIndexFileWriter()
    val conf = spark.sparkContext.conf
    TaskContext.setTaskContext(
      TaskContextImplAdapter.createTaskContextImpl(
        0,
        0,
        0,
        0,
        new TaskMemoryManager(new TestMemoryManager(conf), 0),
        new Properties,
        MetricsSystem.createMetricsSystem(
          "BTreeRecordReaderWriterSuiteV1",
          conf,
          new SecurityManager(conf))))
    val writer = BTreeIndexRecordWriterV1(configuration, fileWriter, schema)

    // Write rows
    nonNullKeyRecords.map(InternalRow(_)).foreach(writer.write(null, _))
    nullKeyRecords.map(InternalRow(_)).foreach(writer.write(null, _))
    writer.close(null)
    // TaskContext.get().asInstanceOf[TaskContextImpl].markTaskCompleted()
    TaskContext.unset()
    val indexData = fileWriter.toByteArray

    // Create reader
    val fileReader = new TestIndexFileReader(indexData)
    val reader = BTreeIndexRecordReaderV1(configuration, schema, fileReader)
    reader.initializeReader()

    val allRows = parseIndexData(reader)
    val nonNullRows = allRows.slice(0, nonNullKeyRecords.length)
    val nullRows = allRows.slice(nonNullKeyRecords.length + 1, nullKeyRecords.length)
    assert(nonNullRows.map(_.getInt(0)) sameElements nonNullKeyRecords)
    nullRows.foreach(key => assert(key.isNullAt(0)))
  }

  // Parse index data and restore the record list.
  // Compare it with original ones to verify consistency.
  private def parseIndexData(
      reader: BTreeIndexRecordReaderV1, debug: Boolean = false): Array[InternalRow] = {

    // Read file meta
    val meta = reader.getFileMeta
    // Read footer
    val footer = reader.readBTreeFooter()

    // Parse Footer
    val nodeCount = footer.getNodesCount

    val allNodeMin = (0 until nodeCount).map(i => footer.getMinValue(i, schema))
    val allNodeMax = (0 until nodeCount).map(i => footer.getMaxValue(i, schema))
    val allNodeRowCount = (0 until nodeCount).map(i => footer.getRowCountOfNode(i))
    val allNodeOffset = (0 until nodeCount).map(i => footer.getNodeOffset(i))
    val allNodeSize = (0 until nodeCount).map(i => footer.getNodeSize(i))

    // Read row id list
    val partSize = reader.rowIdListSizePerSection
    val totalRowCount = footer.getNonNullKeyRecordCount + footer.getNullKeyRecordCount
    val partCount = (totalRowCount / partSize) + (if (totalRowCount % partSize == 0) 0 else 1)
    val rowIdList = (0 until partCount).flatMap { i =>
      val rowIdListPart = reader.readBTreeRowIdList(footer, i)
      val count = if ((i + 1) * partSize > totalRowCount) {
        totalRowCount % partSize
      } else {
        partSize
      }
      (0 until count).map(rowIdListPart.getRowId)
    }
    val nodeFibers = (0 until nodeCount).map { n =>
      reader.readBTreeNodeData(footer, n)
    }
    val uniqueKeys = nodeFibers.flatMap { node =>
      (0 until node.getKeyCount).map(i => (node.getKey(i, schema), node.getRowIdPos(i)))
    }
    val allRows = new Array[InternalRow](totalRowCount)
    uniqueKeys.indices.foreach { i =>
      if (i == uniqueKeys.length - 1) {
        (uniqueKeys(i)._2 until footer.getNonNullKeyRecordCount).foreach { idx =>
          allRows(rowIdList(idx)) = uniqueKeys(i)._1
        }
      } else {
        (uniqueKeys(i)._2 until uniqueKeys(i + 1)._2).foreach { idx =>
          allRows(rowIdList(idx)) = uniqueKeys(i)._1
        }
      }
    }
    (footer.getNonNullKeyRecordCount + 1 until totalRowCount).foreach { idx =>
      allRows(rowIdList(idx)) = InternalRow(null)
    }

    if (debug) {

      val minMaxString = allNodeMin.zip(allNodeMax).map(i => i._1 + "-" + i._2).mkString(" ")
      val rowCountString = allNodeRowCount.mkString(" ")
      val offsetSizeString =
        allNodeOffset.zip(allNodeSize).map(i => "(" + i._1 + "," + i._2 + ")").mkString(" ")

      val nodeString = nodeFibers.map { node =>
        "    Node #" + nodeFibers.indexOf(node) + ": " + (0 until 8).map { i =>
          val rowCount = node.getRowIdPos(i + 1) - node.getRowIdPos(i)
          val rowList =
            (0 until rowCount).map(c => rowIdList(node.getRowIdPos(i) + c)).mkString(",")
          "(" + node.getKey(i, schema) + "," + rowCount + ",[" + rowList + "]" + ")"
        }.mkString(" ")
      }.mkString("\n")

      val parseString =
        s"""
           |Parse Index Data
           |  Index Size: ${meta.fileLength}
           |  Index Footer:
           |    Footer Size: ${meta.footerLength}
           |    Node Count: $nodeCount
           |    Min Max for each Node: $minMaxString
           |    Row Count for each Node: $rowCountString
           |    Offset and Size for each Node: $offsetSizeString
           |    Non Null Key Count: ${footer.getNonNullKeyRecordCount}
           |    Null Key Count: ${footer.getNullKeyRecordCount}
           |  Row Id List:
           |    Row Id List Size: ${meta.rowIdListLength}
           |    Row Id List Data: ${rowIdList.slice(0, 8).mkString(" ")} ...
           |  Nodes: Format: (key,count,rows)
           |$nodeString
      """.stripMargin

      print(parseString)
    }
    allRows
  }
}
