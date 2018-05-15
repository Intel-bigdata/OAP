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

import java.io.ByteArrayOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.{Aggregator, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileWriterImpl
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsWriteManager
import org.apache.spark.sql.execution.datasources.oap.utils.{BTreeNode, BTreeUtils, NonNullKeyWriter}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.{BitSet, OapExternalSorter}

private[index] object BTreeIndexRecordWriter {
  def apply(
      configuration: Configuration,
      indexFile: Path,
      schema: StructType,
      indexVersion: IndexVersion): BTreeIndexRecordWriter = {
    val writer = IndexFileWriterImpl(configuration, indexFile)
    indexVersion match {
      case IndexVersion.OAP_INDEX_V1 =>
        BTreeIndexRecordWriter(configuration, writer, schema)
    }
  }
}

private[index] case class BTreeIndexRecordWriter(
    configuration: Configuration,
    fileWriter: IndexFileWriter,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)
  private lazy val nnkw = new NonNullKeyWriter(keySchema)

  private val combiner: Int => Seq[Int] = Seq(_)
  private val merger: (Seq[Int], Int) => Seq[Int] = _ :+ _
  private val mergeCombiner: (Seq[Int], Seq[Int]) => Seq[Int] = _ ++ _
  private val aggregator =
    new Aggregator[InternalRow, Int, Seq[Int]](combiner, merger, mergeCombiner)
  private val externalSorter = new OapExternalSorter[InternalRow, Int, Seq[Int]](
    TaskContext.get(), Some(aggregator), Some(ordering))
  private var recordCount: Int = 0
  private var nullRecordCount: Int = 0
  private lazy val statisticsManager = new StatisticsWriteManager {
    this.initialize(BTreeIndexType, keySchema, configuration)
  }
  // nullBitSet is less than 33MB
  private lazy val nullBitSet = new BitSet(Int.MaxValue)

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    if (v.anyNull) {
      require(keySchema.length == 1, "No support for multi-column index building with null keys!")
      nullBitSet.set(recordCount)
      nullRecordCount += 1
    } else {
      externalSorter.insert(v, recordCount)
    }
    statisticsManager.addOapKey(v)
    if (recordCount == Int.MaxValue) {
      throw new OapException("Cannot support indexing more than 2G rows!")
    }
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    flush()
    fileWriter.close()
  }
  private def buildOrdering(keySchema: StructType): Ordering[InternalRow] = {
    // here i change to use param id to index_id to get data type in keySchema
    val order = keySchema.zipWithIndex.map {
      case (field, index) => SortOrder(
        BoundReference(index, field.dataType, nullable = true),
        if (!field.metadata.contains("isAscending") || field.metadata.getBoolean("isAscending")) {
          Ascending
        } else {
          Descending
        }
      )
    }
    GenerateOrdering.generate(order, keySchema.toAttributes)
  }

  private lazy val ordering = buildOrdering(keySchema)

  /**
   * Working Flow:
   *  1. Write index magic version into header
   *  2. Split all unique keys into some nodes
   *  3. Serialize nodes and call fileWriter.writeNode()
   *  4. Serialize row id List based on sorted unique keys and call fileWriter.writeRowIdList()
   *  5. Serialize footer and call fileWriter.writeFooter()
   *  5. Write index file meta: footer size, row id list size
   */
  private[index] def flush(): Unit = {
    val sortedIter = externalSorter.iterator
    // Start
    fileWriter.write(IndexUtils.serializeVersion(IndexFile.VERSION_NUM))
    val tempIdWriter = fileWriter.tempRowIdWriter()
    var startPosInRowList = 0
    val nodes = if (sortedIter.nonEmpty) {
      val treeSize = externalSorter.getDistinctCount
      val treeShape = BTreeUtils.generate2(treeSize)
      // Trick here. If root node has no child, then write root node as a child.
      val children = if (treeShape.children.nonEmpty) treeShape.children else treeShape :: Nil

      // Write Node
      children.map { node =>
        val keyCount = sumKeyCount(node) // total number of keys of this node
        if (keyCount == 0) {
          // this node is an empty node
          BTreeNodeMetaData(0, 0, null, null)
        } else {
          val nodeUniqueKeys = sortedIter.take(keyCount).toArray
          val bTreeNodeMetaData =
            serializeNode(nodeUniqueKeys, startPosInRowList, tempIdWriter)
          startPosInRowList += bTreeNodeMetaData.rowCount
          bTreeNodeMetaData
        }
      }
    } else {
      Seq(BTreeNodeMetaData(0, 0, null, null))
    }
    tempIdWriter.close()
    // Write Row Id List
    fileWriter.writeRowId(tempIdWriter)
    var nullIdSize = 0
    nullBitSet.iterator.foreach { x =>
      fileWriter.write(IndexUtils.toBytes(x))
      nullIdSize += IndexUtils.INT_SIZE
    }
    // Write Footer
    val footerBuf = serializeFooter(nullRecordCount, nodes)
    fileWriter.write(footerBuf)
    // Write index file meta: footer size, row id list size
    fileWriter.writeLong(startPosInRowList * IndexUtils.INT_SIZE + nullIdSize)
    fileWriter.writeInt(footerBuf.length)
  }

  /**
   * Layout of node:
   * Field Description                        Byte Size
   * Key Meta Section                         4 + 4 * N
   * Key Count                                 4 Bytes
   * Start Pos for Key #1 in Key Data          4 Bytes
   * Start Pos for Key #1 in Row Id List       4 Bytes
   * ...
   * Start Pos for Key #N in Key Data          4 Bytes
   * Start Pos for Key #N in Row Id List       4 Bytes
   * Key Data Section                         M Bytes
   * Key Data For Key #1
   * ...
   * Key Data For Key #N
   * @param uniqueKeys keys to write into a btree node
   * @param initRowPos the starting position of row ids for this node
   * @param rowIdListWriter the writer to write row id list
   * @return BTreeNodeMetaData
   */
  private[index] def serializeNode(
      uniqueKeys: Array[Product2[InternalRow, Seq[Int]]],
      initRowPos: Int,
      rowIdListWriter: IndexFileWriter): BTreeNodeMetaData = {
    val buffer = new ByteArrayOutputStream()
    val keyBuffer = new ByteArrayOutputStream()

    IndexUtils.writeInt(buffer, uniqueKeys.length)
    var rowPos = 0
    uniqueKeys.foreach { tup =>
      IndexUtils.writeInt(buffer, keyBuffer.size())
      IndexUtils.writeInt(buffer, initRowPos + rowPos)
      nnkw.writeKey(keyBuffer, tup._1)
      rowPos += tup._2.size
      tup._2.foreach { x =>
        rowIdListWriter.writeInt(x)
      }
    }
    val byteArray = buffer.toByteArray ++ keyBuffer.toByteArray
    fileWriter.write(byteArray)
    BTreeNodeMetaData(rowPos, byteArray.length, uniqueKeys.head._1, uniqueKeys.last._1)
  }

  // TODO: BTreeNode can be re-write. It doesn't carry any values.
  private def sumKeyCount(node: BTreeNode): Int = {
    if (node.children.nonEmpty) node.children.map(sumKeyCount).sum else node.root
  }

  /**
   * Layout of Footer:
   * Field Description              Byte Size
   * Index Version Number           4 Bytes
   * Row Count with Non-Null Key    4 Bytes
   * Row Count With Null Key        4 Bytes
   * Node Count                     4 Bytes
   * Nodes Meta Data                Node Count * 20 Bytes
   * Row Count                        4 Bytes
   * Start Pos                        4 Bytes
   * Size In Byte                     4 Bytes
   * Min Key Pos in Key Data          4 Bytes
   * Max Key Pos in Key Data          4 Bytes
   *
   * Statistic info Size              4 Bytes
   * Statistic Info                   X Bytes
   * Key Data - Variable Bytes      M
   * Min Key For Child #1 - Min
   * Max Key For Child #1
   * ...
   * Min Key For Child #N
   * Max Key For Child #N - Max
   * TODO: Make serialize and deserialize(in reader) in same style.
   */
  private def serializeFooter(nullKeyRowCount: Int, nodes: Seq[BTreeNodeMetaData]): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val keyBuffer = new ByteArrayOutputStream()
    val statsBuffer = new ByteArrayOutputStream()

    // Index File Version Number
    IndexUtils.writeInt(buffer, IndexFile.VERSION_NUM)
    // Record Count(all with non-null key) of all nodes in B+ tree
    IndexUtils.writeInt(buffer, nodes.map(_.rowCount).sum)
    // Count of Record(s) that have null key
    IndexUtils.writeInt(buffer, nullKeyRowCount)
    // Child Count
    IndexUtils.writeInt(buffer, nodes.size)

    var offset = 0
    nodes.foreach { node =>
      // Row Count for each Child
      IndexUtils.writeInt(buffer, node.rowCount)
      // Start Pos for each Child
      IndexUtils.writeInt(buffer, offset)
      // Size for each Child
      IndexUtils.writeInt(buffer, node.byteSize)
      // Min Key Pos for each Child
      IndexUtils.writeInt(buffer, keyBuffer.size())
      if (node.min != null) {
        nnkw.writeKey(keyBuffer, node.min)
      }
      // Max Key Pos for each Child
      IndexUtils.writeInt(buffer, keyBuffer.size())
      if (node.max != null) {
        nnkw.writeKey(keyBuffer, node.max)
      }
      offset += node.byteSize
    }
    // the return of write should be equal to statsBuffer.size
    statisticsManager.write(statsBuffer)
    IndexUtils.writeInt(buffer, statsBuffer.size)
    buffer.toByteArray ++ statsBuffer.toByteArray ++ keyBuffer.toByteArray
  }
}

private case class BTreeNodeMetaData(
    rowCount: Int,
    byteSize: Int,
    min: InternalRow,
    max: InternalRow)
