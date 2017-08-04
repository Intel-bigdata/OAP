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

package org.apache.spark.sql.execution.datasources.oap.io

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}


/**
 * Read the index file into memory, and can be accessed as [[ChunkedByteBuffer]].
 */
private[oap] case class IndexFile(file: Path) {
  private def putToFiberCache(buf: Array[Byte]): ChunkedByteBuffer = {
    // TODO: make it configurable
    val cbbos = new ChunkedByteBufferOutputStream(buf.length, ByteBuffer.allocate)
    cbbos.write(buf)
    cbbos.close()
    cbbos.toChunkedByteBuffer
  }

  def getIndexFiberData(conf: Configuration): ChunkedByteBuffer = {
    val fs = file.getFileSystem(conf)
    val fin = fs.open(file)
    // wind to end of file to get tree root
    // TODO check if enough to fit in Int
    val fileLength = fs.getContentSummary(file).getLength
    val bytes = new Array[Byte](fileLength.toInt)

    fin.readFully(0, bytes)
    fin.close()
    // TODO partial cached index fiber
    putToFiberCache(bytes)
  }

  def version(conf: Configuration): Int = {
    val fs = file.getFileSystem(conf)
    val fin = fs.open(file)
    val bytes = new Array[Byte](8)
    fin.read(bytes, 0, 8)
    fin.close()
    (bytes(6) << 8) + bytes(7)
  }
}

private[oap] object IndexFile {
  val indexFileHeaderLength = 8
  val INDEX_VERSION = 1
}
