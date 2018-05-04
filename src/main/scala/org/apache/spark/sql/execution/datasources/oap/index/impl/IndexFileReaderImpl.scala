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

package org.apache.spark.sql.execution.datasources.oap.index.impl

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, MemoryManager}
import org.apache.spark.sql.execution.datasources.oap.index.IndexFileReader

private[index] case class IndexFileReaderImpl(
    configuration: Configuration,
    indexPath: Path) extends IndexFileReader {

  private val fileStatus = indexPath.getFileSystem(configuration).getFileStatus(indexPath)

  override protected val is: FSDataInputStream =
    indexPath.getFileSystem(configuration).open(indexPath)

  override def readFiberCache(position: Long, length: Int): FiberCache =
    MemoryManager.toIndexFiberCache(is, position, length)

  override def read(position: Long, length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    readFully(position, bytes)
    bytes
  }

  override def readFully(position: Long, buf: Array[Byte]): Unit = is.readFully(position, buf)

  override def getLen: Long = fileStatus.getLen

  override def getName: String = indexPath.toString

  override def close(): Unit = is.close()
}
