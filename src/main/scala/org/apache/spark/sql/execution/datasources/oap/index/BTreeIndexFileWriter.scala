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

import org.apache.spark.sql.execution.datasources.oap.io.IndexFile

/**
 * BTreeIndexFile Structure
 * Field                          Description
 * Version                        OAPIDX02
 * Node #1                        Part #1 of all sorted key list
 * Node #2
 * ...
 * Node #N
 * Row ID List                    Row ID List sorted by corresponding key
 * Footer                         Meta data for this index file
 * Row Id List Section Size       4 bytes
 * Footer Section Size            4 bytes
 */
private case class BTreeIndexFileWriter(
    configuration: Configuration,
    file: Path) {

  private lazy val writer = file.getFileSystem(configuration).create(file, true)

  private var rowIdListSize = 0
  private var footerSize = 0

  def start(): Unit = {
    IndexUtils.writeHead(writer, IndexFile.INDEX_VERSION)
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
    littleEndianWriter.writeInt(rowIdListSize)
    littleEndianWriter.writeInt(footerSize)
  }

  def close(): Unit = writer.close()
}
