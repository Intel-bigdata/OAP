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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.parquet.format.CompressionCodec

import org.apache.spark.{Aggregator, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileWriterImpl
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsWriteManager
import org.apache.spark.sql.execution.datasources.oap.utils.{BTreeNode, BTreeUtils, NonNullKeyWriter}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.{BitSet, OapExternalSorter}

private[index] object BTreeIndexRecordWriter {
  def apply(
      configuration: Configuration,
      indexFile: Path,
      schema: StructType,
      codec: CompressionCodec,
      indexVersion: IndexVersion): BTreeIndexRecordWriter = {
    val writer = IndexFileWriterImpl(configuration, indexFile)
    indexVersion match {
      case IndexVersion.OAP_INDEX_V1 =>
        BTreeIndexRecordWriterV1(configuration, writer, schema)
      case IndexVersion.OAP_INDEX_V2 =>
        BTreeIndexRecordWriterV2(configuration, writer, schema, codec)
    }
  }
}

abstract class BTreeIndexRecordWriter(
    configuration: Configuration,
    fileWriter: IndexFileWriter,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  // abstract member and function
  protected val VERSION_NUM: Int

  protected def compressData(bytes: Array[Byte]): Array[Byte]

  protected def writeCompressCodec(writer: IndexFileWriter): Unit

  protected def serializeFooter(nullKeyRowCount: Int, nodes: Seq[BTreeNodeMetaData]): Array[Byte]

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)
  @transient protected lazy val nnkw = new NonNullKeyWriter(keySchema)

  private val combiner: Int => Seq[Int] = Seq(_)
  private val merger: (Seq[Int], Int) => Seq[Int] = _ :+ _
  private val mergeCombiner: (Seq[Int], Seq[Int]) => Seq[Int] = _ ++ _
  private val aggregator =
    new Aggregator[InternalRow, Int, Seq[Int]](combiner, merger, mergeCombiner)
  private val externalSorter = {
    val taskContext = TaskContext.get()
    val sorter = new OapExternalSorter[InternalRow, Int, Seq[Int]](
      taskContext, Some(aggregator), Some(ordering))
    taskContext.addTaskCompletionListener(_ => sorter.stop())
    sorter
  }
  private var recordCount: Int = 0
  private var nullRecordCount: Int = 0
  protected lazy val statisticsManager: StatisticsWriteManager = new StatisticsWriteManager {
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
    fileWriter.write(IndexUtils.serializeVersion(VERSION_NUM))
    val tempIdWriter = fileWriter.tempRowIdWriter()
    val rowIdListBuffer = new ByteArrayOutputStream()
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
            serializeNode(nodeUniqueKeys, startPosInRowList, tempIdWriter, rowIdListBuffer)
          startPosInRowList += bTreeNodeMetaData.rowCount
          bTreeNodeMetaData
        }
      }
    } else {
      Seq(BTreeNodeMetaData(0, 0, null, null))
    }
    var nullIdSize = 0
    nullBitSet.iterator.foreach { x =>
      // fileWriter.write(IndexUtils.toBytes(x))
      storeRowId(x, rowIdListBuffer, tempIdWriter)
      nullIdSize += IndexUtils.INT_SIZE
    }
    val bytes = compressData(rowIdListBuffer.toByteArray)
    tempIdWriter.write(bytes)
    rowIdListPartLengthArray.append(bytes.length)
    rowIdListBuffer.close()
    tempIdWriter.close()
    // Write Row Id List
    fileWriter.writeRowId(tempIdWriter)

    val partNum = recordCount / rowIdListSizePerSection +
        (if (recordCount % rowIdListSizePerSection == 0) 0 else 1)
    assert(rowIdListPartLengthArray.length == partNum)
    // Write Footer
    val footerBuf = compressData(serializeFooter(nullRecordCount, nodes))
    fileWriter.write(footerBuf)
    // Write index file meta: footer size, row id list size
    fileWriter.writeLong(rowIdListPartLengthArray.sum)
    fileWriter.writeInt(footerBuf.length)
    writeCompressCodec(fileWriter)
  }

  protected val rowIdListSizePerSection: Int =
    configuration.getInt(OapConf.OAP_BTREE_ROW_LIST_PART_SIZE.key, 1024 * 1024)
  protected val rowIdListPartLengthArray = new ArrayBuffer[Int]()

  private def storeRowId(id: Int, buf: ByteArrayOutputStream, writer: IndexFileWriter): Unit = {
    if (buf.size() == rowIdListSizePerSection * IndexUtils.INT_SIZE) {
      val bytes = compressData(buf.toByteArray)
      writer.write(bytes)
      buf.reset()
      rowIdListPartLengthArray.append(bytes.length)
    }
    IndexUtils.writeInt(buf, id)
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
   *
   * @param uniqueKeys      keys to write into a btree node
   * @param initRowPos      the starting position of row ids for this node
   * @param rowIdListWriter the writer to write row id list
   * @return BTreeNodeMetaData
   */
  private def serializeNode(
      uniqueKeys: Array[Product2[InternalRow, Seq[Int]]],
      initRowPos: Int,
      rowIdListWriter: IndexFileWriter,
      rowIdListBuffer: ByteArrayOutputStream): BTreeNodeMetaData = {
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
        storeRowId(x, rowIdListBuffer, rowIdListWriter)
        // rowIdListWriter.writeInt(x)
      }
    }
    val byteArray = compressData(buffer.toByteArray ++ keyBuffer.toByteArray)
    fileWriter.write(byteArray)
    BTreeNodeMetaData(rowPos, byteArray.length, uniqueKeys.head._1, uniqueKeys.last._1)
  }

  // TODO: BTreeNode can be re-write. It doesn't carry any values.
  private def sumKeyCount(node: BTreeNode): Int = {
    if (node.children.nonEmpty) node.children.map(sumKeyCount).sum else node.root
  }

  protected case class BTreeNodeMetaData(
      rowCount: Int,
      byteSize: Int,
      min: InternalRow,
      max: InternalRow)

}
