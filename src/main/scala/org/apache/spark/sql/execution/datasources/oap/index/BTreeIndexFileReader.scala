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
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.parquet.format.CompressionCodec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, MemoryManager}
import org.apache.spark.sql.execution.datasources.oap.io.{CodecFactory, IndexFile}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ShutdownHookManager

private[oap] case class BTreeIndexFileReader(
    configuration: Configuration,
    file: Path) extends Logging {

  private val VERSION_SIZE = IndexFile.VERSION_LENGTH
  private val FOOTER_LENGTH_SIZE = IndexUtils.INT_SIZE
  private val ROW_ID_LIST_LENGTH_SIZE = IndexUtils.LONG_SIZE
  private val CODEC_SIZE = IndexUtils.INT_SIZE
  private val META_SIZE = FOOTER_LENGTH_SIZE + ROW_ID_LIST_LENGTH_SIZE + CODEC_SIZE

  // Section ID for fiber cache reading.
  val footerSectionId: Int = 0
  val rowIdListSectionId: Int = 1
  val nodeSectionId: Int = 2

  val rowIdListSizePerSection: Int =
    configuration.getInt(OapConf.OAP_BTREE_ROW_LIST_PART_SIZE.key, 1024 * 1024)

  private lazy val (reader, fileLength) = {
    val fs = file.getFileSystem(configuration)
    (fs.open(file), fs.getFileStatus(file).getLen)
  }

  private val (footerLength, rowIdListLength, codec) = {
    val sectionLengthIndex = fileLength - META_SIZE
    val sectionLengthBuffer = new Array[Byte](META_SIZE)
    reader.readFully(sectionLengthIndex, sectionLengthBuffer)
    val rowIdListSize = getLongFromBuffer(sectionLengthBuffer, 0)
    val footerSize = getIntFromBuffer(sectionLengthBuffer, ROW_ID_LIST_LENGTH_SIZE)
    val codecValue =
      getIntFromBuffer(sectionLengthBuffer, ROW_ID_LIST_LENGTH_SIZE + FOOTER_LENGTH_SIZE)
    val codec = CompressionCodec.findByValue(codecValue)
    (footerSize, rowIdListSize, codec)
  }

  @transient private val decompressor = new CodecFactory(configuration).getDecompressor(codec)

  private def footerIndex = fileLength - META_SIZE - footerLength
  private def rowIdListIndex = footerIndex - rowIdListLength
  private def nodesIndex = VERSION_SIZE

  private def getLongFromBuffer(buffer: Array[Byte], offset: Int) =
    Platform.getLong(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  private def getIntFromBuffer(buffer: Array[Byte], offset: Int) =
    Platform.getInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset)

  private def readData(in: FSDataInputStream, position: Long, length: Int): Array[Byte] = {

    assert(length <= Int.MaxValue, "Try to read too large index data")

    val bytes = new Array[Byte](length)
    in.readFully(position, bytes)
    IndexUtils.decompressIndexData(decompressor, bytes)
  }

  def checkVersionNum(versionNum: Int): Unit = {
    if (IndexFile.VERSION_NUM != versionNum) {
      throw new OapException("Btree Index File version is not compatible!")
    }
  }

  def readFooter(): FiberCache =
    MemoryManager.putToIndexFiberCache(readData(reader, footerIndex, footerLength))

  def readRowIdList(offset: Int, size: Int): FiberCache = {
    MemoryManager.putToIndexFiberCache(readData(reader, rowIdListIndex + offset, size))
  }

  def readNode(offset: Int, size: Int): FiberCache =
    MemoryManager.putToIndexFiberCache(readData(reader, nodesIndex + offset, size))

  def close(): Unit = try {
    reader.close()
  } catch {
    case e: Exception =>
      if (!ShutdownHookManager.inShutdown()) {
        logWarning("Exception in FSDataInputStream.close()", e)
      }
  }
}
