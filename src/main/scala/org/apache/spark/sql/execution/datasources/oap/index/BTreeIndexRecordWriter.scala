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
import java.util.Comparator

import scala.collection.JavaConverters._

import com.google.common.collect.ArrayListMultimap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.parquet.bytes.LittleEndianDataOutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.utils.{BTreeNode, BTreeUtils}
import org.apache.spark.sql.types._

private[index] case class BTreeIndexRecordWriter(
    configuration: Configuration,
    fileWriter: BTreeIndexFileWriter,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private val multiHashMap = ArrayListMultimap.create[InternalRow, Int]()
  private var recordCount: Int = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    multiHashMap.put(v, recordCount)
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    flush()
    fileWriter.close()
  }

  private[index] def sortUniqueKeys(): Array[InternalRow] = {
    def buildOrdering(keySchema: StructType): Ordering[InternalRow] = {
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

    lazy val ordering = buildOrdering(keySchema)
    val partitionUniqueSize = multiHashMap.keySet().size()
    val uniqueKeys = multiHashMap.keySet().toArray(new Array[InternalRow](partitionUniqueSize))
    assert(uniqueKeys.size == partitionUniqueSize)
    lazy val comparator: Comparator[InternalRow] = new Comparator[InternalRow]() {
      override def compare(o1: InternalRow, o2: InternalRow): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          -1
        } else if (o2 == null) {
          1
        } else {
          ordering.compare(o1, o2)
        }
      }
    }
    // sort keys
    java.util.Arrays.sort(uniqueKeys, comparator)
    uniqueKeys
  }

  /**
   * Working Flow:
   *  1. Call fileWriter.start() to write some Index Info
   *  2. Split all unique keys into some nodes
   *  3. Serialize nodes and call fileWriter.writeNode()
   *  4. Serialize row id List based on sorted unique keys and call fileWriter.writeRowIdList()
   *  5. Serialize footer and call fileWriter.writeFooter()
   *  5. Call fileWriter.end() to write some meta data (e.g. file offset for each section)
   */
  private[index] def flush(): Unit = {
    val uniqueKeys = sortUniqueKeys()
    val treeShape = BTreeUtils.generate2(uniqueKeys.length)
    // Trick here. If root node has no child, then write root node as a child.
    val children = if (treeShape.children.nonEmpty) treeShape.children else treeShape :: Nil

    // Start
    fileWriter.start()
    // Write Node
    var startPosInKeyList = 0
    var startPosInRowList = 0
    val nodes =
      children.map { node =>
        val keyCount = sumKeyCount(node)
        val nodeUniqueKeys = uniqueKeys.slice(startPosInKeyList, startPosInKeyList + keyCount)
        val rowCount = nodeUniqueKeys.map(multiHashMap.get(_).size()).sum

        val nodeBuf = serializeNode(nodeUniqueKeys, startPosInRowList)
        fileWriter.writeNode(nodeBuf)
        startPosInKeyList += keyCount
        startPosInRowList += rowCount
        BTreeNodeMetaData(rowCount, nodeBuf.length, nodeUniqueKeys.head, nodeUniqueKeys.last)
      }
    // Write Row Id List
    fileWriter.writeRowIdList(serializeRowIdLists(uniqueKeys))
    // Write Footer
    fileWriter.writeFooter(serializeFooter(nodes))
    // End
    fileWriter.end()
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
   */
  private[index] def serializeNode(
      uniqueKeys: Array[InternalRow], startPosInRowList: Int): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val output = new LittleEndianDataOutputStream(buffer)
    val keyBuffer = new ByteArrayOutputStream()
    val keyOutput = new LittleEndianDataOutputStream(keyBuffer)

    output.writeInt(uniqueKeys.length)
    var rowPos = startPosInRowList
    uniqueKeys.foreach { key =>
      output.writeInt(keyBuffer.size())
      output.writeInt(rowPos)
      IndexUtils.writeBasedOnSchema(keyOutput, key, keySchema)
      rowPos += multiHashMap.get(key).size()
    }
    buffer.toByteArray ++ keyBuffer.toByteArray
  }

  // TODO: BTreeNode can be re-write. It doesn't carry any values.
  private def sumKeyCount(node: BTreeNode): Int = {
    if (node.children.nonEmpty) node.children.map(sumKeyCount).sum
    else node.root
  }

  /**
   * Example of Row Id List:
   * Row Id: 0 1 2 3 4 5 6 7 8 9
   * Key:    1 2 3 4 1 2 3 4 1 2
   * Then Row Id List is Stored as: 0481592637
   */
  private def serializeRowIdLists(uniqueKeys: Array[InternalRow]): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val out = new LittleEndianDataOutputStream(buffer)
    uniqueKeys.flatMap(key => multiHashMap.get(key).asScala).foreach(out.writeInt)
    buffer.toByteArray
  }

  /**
   * Layout of Footer:
   * Field Description              Byte Size
   * Row Count                      4 Bytes
   * Node Count                     4 Bytes
   * Nodes Meta Data                Node Count * 20 Bytes
   * Row Count                        4 Bytes
   * Start Pos                        4 Bytes
   * Size In Byte                     4 Bytes
   * Min Key Pos in Key Data          4 Bytes
   * Max Key Pos in Key Data          4 Bytes
   * Key Data - Variable Bytes      M
   * Min Key For Child #1 - Min
   * Max Key For Child #1
   * ...
   * Min Key For Child #N
   * Max Key For Child #N - Max
   * TODO: Make serialize and deserialize(in reader) in same style.
   */
  private def serializeFooter(nodes: Seq[BTreeNodeMetaData]): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val output = new LittleEndianDataOutputStream(buffer)

    val keyBuffer = new ByteArrayOutputStream()
    val keyOutput = new LittleEndianDataOutputStream(keyBuffer)

    // Record Count
    output.writeInt(nodes.map(_.rowCount).sum)
    // Child Count
    output.writeInt(nodes.size)

    var offset = 0
    nodes.foreach { node =>
      // Row Count for each Child
      output.writeInt(node.rowCount)
      // Start Pos for each Child
      output.writeInt(offset)
      // Size for each Child
      output.writeInt(node.byteSize)
      // Min Key Pos for each Child
      output.writeInt(keyBuffer.size())
      IndexUtils.writeBasedOnSchema(keyOutput, node.min, keySchema)
      // Max Key Pos for each Child
      output.writeInt(keyBuffer.size())
      IndexUtils.writeBasedOnSchema(keyOutput, node.max, keySchema)
      offset += node.byteSize
    }
    buffer.toByteArray ++ keyBuffer.toByteArray
  }
}

private case class
BTreeNodeMetaData(rowCount: Int, byteSize: Int, min: InternalRow, max: InternalRow)
