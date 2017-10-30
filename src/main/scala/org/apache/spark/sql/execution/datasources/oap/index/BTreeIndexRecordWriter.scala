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

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.Comparator

import scala.collection.JavaConverters._

import com.google.common.collect.ArrayListMultimap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.parquet.bytes.LittleEndianDataOutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager
import org.apache.spark.sql.execution.datasources.oap.utils.{BTreeNode, BTreeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[index] case class BTreeIndexRecordWriter(
    configuration: Configuration,
    writer: OutputStream,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  // TODO: Pass index file Path
  private var fileWriter: BTreeIndexFileWriter = _
  // Just a temp function to inject fileWriter. Since I can't change class parameter list before
  // Code integration.
  def setFileWriter(writer: BTreeIndexFileWriter): Unit = fileWriter = writer

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private val multiHashMap = ArrayListMultimap.create[InternalRow, Int]()
  private var recordCount: Int = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    multiHashMap.put(v, recordCount)
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    // TODO: Change to flush() after new reader is ready
    flushToFile(writer)
    writer.close()
  }

  private def sortUniqueKeys(): Array[InternalRow] = {
    def buildOrdering(keySchema: StructType): Ordering[InternalRow] = {
      // here i change to use param id to index_id to get datatype in keySchema
      val order = keySchema.zipWithIndex.map {
        case (field, index) => SortOrder(
          BoundReference(index, field.dataType, nullable = true),
          if (!field.metadata.contains("isAscending") || field.metadata.getBoolean("isAscending")) {
            Ascending
          } else {
            Descending
          })
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

  def flushToFile(out: OutputStream): Unit = {

    val uniqueKeys = sortUniqueKeys()
    val partitionUniqueSize = uniqueKeys.length
    // build index file
    var i = 0
    var fileOffset = 0L
    val offsetMap = new java.util.HashMap[InternalRow, Long]()
    fileOffset += writeHead(out, IndexFile.INDEX_VERSION)
    // write data segment.
    while (i < partitionUniqueSize) {
      offsetMap.put(uniqueKeys(i), fileOffset)
      val rowIds = multiHashMap.get(uniqueKeys(i))
      // row count for same key
      IndexUtils.writeInt(out, rowIds.size())
      // 4 -> value1, stores rowIds count.
      fileOffset = fileOffset + 4
      var idIter = 0
      while (idIter < rowIds.size()) {
        IndexUtils.writeLong(out, rowIds.get(idIter))
        // 8 -> value2, stores a row id
        fileOffset = fileOffset + 8
        idIter = idIter + 1
      }
      i = i + 1
    }
    val dataEnd = fileOffset
    // write index segment.
    val treeShape = BTreeUtils.generate2(partitionUniqueSize)
    val uniqueKeysList = new java.util.LinkedList[InternalRow]()
    import scala.collection.JavaConverters._
    uniqueKeysList.addAll(uniqueKeys.toSeq.asJava)

    val treeOffset = writeTreeToOut(treeShape, out, offsetMap,
      fileOffset, uniqueKeysList, keySchema, 0, -1L)

    val statisticsManager = new StatisticsManager
    statisticsManager.initialize(BTreeIndexType, keySchema, configuration)
    statisticsManager.write(out)

    assert(uniqueKeysList.size == 1)
    IndexUtils.writeLong(out, dataEnd + treeOffset._1)
    IndexUtils.writeLong(out, dataEnd)
    IndexUtils.writeLong(out, offsetMap.get(uniqueKeysList.getFirst))
  }

  /**
   * Write tree to output, return bytes written and updated nextPos
   */
  private def writeTreeToOut(
      tree: BTreeNode,
      writer: OutputStream,
      map: java.util.HashMap[InternalRow, Long],
      fileOffset: Long,
      keysList: java.util.LinkedList[InternalRow],
      keySchema: StructType,
      listOffsetFromEnd: Int,
      nextP: Long): (Long, Long) = {
    if (tree.children.nonEmpty) {
      var subOffset = 0L
      // this is a non-leaf node
      // Need to write down all subtrees
      val childrenCount = tree.children.size
      assert(childrenCount == tree.root)
      var iter = childrenCount
      var currentNextPos = nextP
      // write down all subtrees reversely
      while (iter > 0) {
        iter -= 1
        val subTree = tree.children(iter)
        val subListOffsetFromEnd = listOffsetFromEnd + childrenCount - 1 - iter
        val (writeOffset, newNext) = writeTreeToOut(
          subTree, writer, map, fileOffset + subOffset,
          keysList, keySchema, subListOffsetFromEnd, currentNextPos)
        currentNextPos = newNext
        subOffset += writeOffset
      }
      (subOffset + writeIndexNode(
        tree, writer, map, keysList, listOffsetFromEnd, subOffset + fileOffset, -1L
      ), currentNextPos)
    } else {
      (writeIndexNode(
        tree, writer, map, keysList, listOffsetFromEnd, fileOffset, nextP), fileOffset)
    }
  }

  @transient private lazy val projector = UnsafeProjection.create(keySchema)

  /**
   * write file correspond to [[UnsafeIndexNode]]
   */
  private def writeIndexNode(
      tree: BTreeNode,
      writer: OutputStream,
      map: java.util.HashMap[InternalRow, Long],
      keysList: java.util.LinkedList[InternalRow],
      listOffsetFromEnd: Int,
      updateOffset: Long,
      nextPointer: Long): Long = {
    var subOffset = 0
    // write road sign count on every node first
    IndexUtils.writeInt(writer, tree.root)
    // 4 -> value3, stores tree branch count
    subOffset = subOffset + 4
    IndexUtils.writeLong(writer, nextPointer)
    // 8 -> value4, stores next offset
    subOffset = subOffset + 8
    // For all IndexNode, write down all road sign, each pointing to specific data segment
    val start = keysList.size - listOffsetFromEnd - tree.root
    val end = keysList.size - listOffsetFromEnd
    val writeList = keysList.subList(start, end).asScala
    val keyBuf = new ByteArrayOutputStream()
    // offset0 pointer0, offset1 pointer1, ..., offset(n-1) pointer(n-1),
    // len0 key0, len1 key1, ..., len(n-1) key(n-1)
    // 16 <- value5
    val baseOffset = updateOffset + subOffset + tree.root * 16
    var i = 0
    while (i < tree.root) {
      val writeKey = writeList(i)
      IndexUtils.writeLong(writer, baseOffset + keyBuf.size)
      // assert(map.containsKey(writeList(i)))
      IndexUtils.writeLong(writer, map.get(writeKey))
      // 16 -> value5, stores 2 long values, key offset and child offset
      subOffset += 16
      val writeRow = projector.apply(writeKey)
      IndexUtils.writeInt(keyBuf, writeRow.getSizeInBytes)
      writeRow.writeToStream(keyBuf, null)
      i += 1
    }
    writer.write(keyBuf.toByteArray)
    subOffset += keyBuf.size
    map.put(writeList.head, updateOffset)
    var rmCount = tree.root
    while (rmCount > 1) {
      rmCount -= 1
      keysList.remove(keysList.size - listOffsetFromEnd - rmCount)
    }
    subOffset
  }

  private def writeHead(writer: OutputStream, version: Int): Int = {
    writer.write("OAPIDX".getBytes("UTF-8"))
    assert(version <= 65535)
    val data = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(data)
    IndexFile.indexFileHeaderLength
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
  private def serializeNode(uniqueKeys: Array[InternalRow], startPosInRowList: Int): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val output = new LittleEndianDataOutputStream(buffer)
    val keyBuffer = new ByteArrayOutputStream()
    val keyOutput = new LittleEndianDataOutputStream(keyBuffer)

    output.writeInt(uniqueKeys.length)
    var rowPos = startPosInRowList
    uniqueKeys.foreach { key =>
      output.writeInt(keyBuffer.size())
      output.writeInt(rowPos)
      BTreeIndexRecordWriter.writeBasedOnSchema(keyOutput, key, keySchema)
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
      BTreeIndexRecordWriter.writeBasedOnSchema(keyOutput, node.min, keySchema)
      // Max Key Pos for each Child
      output.writeInt(keyBuffer.size())
      BTreeIndexRecordWriter.writeBasedOnSchema(keyOutput, node.max, keySchema)
      offset += node.byteSize
    }
    buffer.toByteArray ++ keyBuffer.toByteArray
  }
}

private[index] object BTreeIndexRecordWriter {

  private[index] def writeBasedOnSchema(
      writer: LittleEndianDataOutputStream, row: InternalRow, schema: StructType): Unit = {
    schema.zipWithIndex.foreach {
      case (field, index) => writeBasedOnDataType(writer, row.get(index, field.dataType))
    }
  }

  private[index] def writeBasedOnDataType(
      writer: LittleEndianDataOutputStream,
      value: Any): Unit = {
    value match {
      case int: Boolean => writer.writeBoolean(int)
      case short: Short => writer.writeShort(short)
      case byte: Byte => writer.writeByte(byte)
      case int: Int => writer.writeInt(int)
      case long: Long => writer.writeLong(long)
      case float: Float => writer.writeFloat(float)
      case double: Double => writer.writeDouble(double)
      case string: UTF8String =>
        val bytes = string.getBytes
        writer.writeInt(bytes.length)
        writer.write(bytes)
      case binary: Array[Byte] =>
        writer.writeInt(binary.length)
        writer.write(binary)
      case other => throw new OapException(s"not support data type $other")
    }
  }
}

private case class BTreeNodeMetaData(
    rowCount: Int, byteSize: Int, min: InternalRow, max: InternalRow)
