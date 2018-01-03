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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, MemoryManager}
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.Platform

private[oap] case class BTreeIndexFileReader(
    configuration: Configuration,
    file: Path) {

  private val VERSION_SIZE = IndexFile.VERSION_LENGTH
  private val FOOTER_LENGTH_SIZE = IndexUtils.INT_SIZE
  private val ROW_ID_LIST_LENGTH_SIZE = IndexUtils.LONG_SIZE

  // Section ID for fiber cache reading.
  val footerSectionId: Int = 0
  val rowIdListSectionId: Int = 1
  val nodeSectionId: Int = 2

  val rowIdListSizePerSection: Int =
    configuration.getInt(SQLConf.OAP_BTREE_ROW_LIST_PART_SIZE.key, 1024 * 1024)

  private val (reader, fileLength) = {
    val fs = file.getFileSystem(configuration)
    (fs.open(file), fs.getFileStatus(file).getLen)
  }

  private val (footerLength, rowIdListLength) = {
    val sectionLengthIndex = fileLength - FOOTER_LENGTH_SIZE - ROW_ID_LIST_LENGTH_SIZE
    val sectionLengthBuffer = new Array[Byte](FOOTER_LENGTH_SIZE + ROW_ID_LIST_LENGTH_SIZE)
    reader.readFully(sectionLengthIndex, sectionLengthBuffer)
    val rowIdListSize = getLongFromBuffer(sectionLengthBuffer, 0)
    val footerSize = getIntFromBuffer(sectionLengthBuffer, ROW_ID_LIST_LENGTH_SIZE)
    (footerSize, rowIdListSize)
  }

  private def footerIndex = fileLength - FOOTER_LENGTH_SIZE - ROW_ID_LIST_LENGTH_SIZE - footerLength
  private def rowIdListIndex = footerIndex - rowIdListLength
  private def nodesIndex = VERSION_SIZE

  private def getLongFromBuffer(buffer: Array[Byte], offset: Int) =
    Platform.getLong(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  private def getIntFromBuffer(buffer: Array[Byte], offset: Int) =
    Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  def checkVersionNum(versionNum: Int): Unit = {
    if (IndexFile.VERSION_NUM != versionNum) {
      reader.close()
      throw new OapException("Btree Index File version is not compatible!")
    }
  }

  def readFooter(): FiberCache =
    MemoryManager.putToIndexFiberCache(reader, footerIndex, footerLength)

  def readRowIdList(partIdx: Int): FiberCache = {
    val partSize = (rowIdListSizePerSection * IndexUtils.INT_SIZE).toLong
    val readLength = if (partIdx * partSize + partSize > rowIdListLength) {
      rowIdListLength % partSize
    } else {
      partSize
    }
    MemoryManager.putToIndexFiberCache(reader, rowIdListIndex + partIdx * partSize, readLength)
  }

  @deprecated("no need to read the whole row id list", "v0.3")
  def readRowIdList(): FiberCache =
    MemoryManager.putToIndexFiberCache(reader, rowIdListIndex, rowIdListLength)

  def readNode(offset: Int, size: Int): FiberCache =
    MemoryManager.putToIndexFiberCache(reader, nodesIndex + offset, size)

  def close(): Unit = reader.close()
}
