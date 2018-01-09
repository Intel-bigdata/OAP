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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, MemoryManager}

private[oap] trait CommonIndexFile {
  def file: Path
  def version(conf: Configuration): Int = {
    val fs = file.getFileSystem(conf)
    val fin = fs.open(file)
    val bytes = new Array[Byte](8)
    fin.readFully(bytes, 0, 8)
    fin.close()
    (bytes(6) << 8) + bytes(7)
  }
}
/**
 * Read the index file into memory, and can be accessed as [[FiberCache]].
 */
private[oap] case class IndexFile(file: Path) extends CommonIndexFile {
  def getIndexFiberData(conf: Configuration): FiberCache = {
    val fs = file.getFileSystem(conf)
    val fin = fs.open(file)
    // wind to end of file to get tree root
    // TODO check if enough to fit in Int
    val fileLength = fs.getContentSummary(file).getLength

    val fiberCache = MemoryManager.putToIndexFiberCache(fin, 0, fileLength.toInt)
    fin.close()
    fiberCache
  }
}

private[oap] object IndexFile {
  val VERSION_LENGTH = 8
  val VERSION_PREFIX = "OAPIDX"
  val VERSION_NUM = 1
}
