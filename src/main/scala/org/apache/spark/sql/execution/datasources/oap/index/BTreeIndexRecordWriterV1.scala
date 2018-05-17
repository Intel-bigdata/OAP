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

import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.types.StructType

private[index] case class BTreeIndexRecordWriterV1(
    configuration: Configuration,
    fileWriter: IndexFileWriter,
    keySchema: StructType)
  extends BTreeIndexRecordWriter(configuration, fileWriter, keySchema) {

  override protected val VERSION_NUM: Int = IndexVersion.OAP_INDEX_V1.id

  override protected def compressData(bytes: Array[Byte]): Array[Byte] = bytes

  override protected def writeCompressCodec(writer: IndexFileWriter): Unit = {
    // DO NOTHING
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
  override protected def serializeFooter(
      nullKeyRowCount: Int,
      nodes: Seq[BTreeNodeMetaData]): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val keyBuffer = new ByteArrayOutputStream()
    val statsBuffer = new ByteArrayOutputStream()

    // Index File Version Number
    IndexUtils.writeInt(buffer, VERSION_NUM)
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
