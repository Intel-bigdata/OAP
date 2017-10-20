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
import org.apache.parquet.bytes.LittleEndianDataOutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile

private case class BTreeIndexFileWriter(
    configuration: Configuration,
    file: Path) {

  private val writer = file.getFileSystem(configuration).create(file, true)

  private var rowIdListSize = 0
  private var footerSize = 0
  private def writeVersion(version: Int): Unit = {
    writer.write("OAPIDX".getBytes("UTF-8"))
    assert(version <= 65535)
    val data = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(data)
  }

  def start(): Unit = {
    writeVersion(IndexFile.INDEX_VERSION)
  }

  def writeNode(buf: Array[Byte]): Unit = {
    writer.write(buf)
  }

  def writeRowIdList(buf: Array[Byte]): Unit = {
    writer.write(buf)
    rowIdListSize = buf.length
  }

  def writeFooter(footer: Array[Byte]): Unit = {
    writer.write(footer)
    footerSize = footer.length
  }

  def end(): Unit = {
    val littleEndianWriter = new LittleEndianDataOutputStream(writer)
    littleEndianWriter.write(rowIdListSize)
    littleEndianWriter.write(footerSize)
  }

  def close(): Unit = writer.close()
}
