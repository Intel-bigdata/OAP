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

package org.apache.spark.sql.execution.datasources.spinach

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.unsafe.Platform

/**
 * Read the index file into memory(offheap), and can be accessed as [[IndexFiberCacheData]].
 */
private[spinach] case class IndexFile(file: Path) {
  def putToFiberCache(buf: Array[Byte]): FiberCacheData = {
    // TODO: make it configurable
    val fiberCacheData = MemoryManager.allocate(buf.length)
    Platform.copyMemory(
      buf, Platform.BYTE_ARRAY_OFFSET, fiberCacheData.fiberData.getBaseObject,
      fiberCacheData.fiberData.getBaseOffset, buf.length)
    fiberCacheData
  }

  def getIndexFiberData(conf: Configuration): IndexFiberCacheData = {
    val fs = file.getFileSystem(conf)
    val fin = fs.open(file)
    // wind to end of file to get tree root
    // TODO check if enough to fit in Int
    val fileLength = fs.getContentSummary(file).getLength
    var bytes = new Array[Byte](fileLength.toInt)
    // fin.read(bytes, 0, fileLength.toInt)
    fin.readFully(0, bytes)
    val offHeapMem = putToFiberCache(bytes)
    bytes = null

    val baseObj = offHeapMem.fiberData.getBaseObject
    val baseOff = offHeapMem.fiberData.getBaseOffset
    val dataEnd = Platform.getLong(baseObj, baseOff + fileLength - 16)
    val rootOffset = Platform.getLong(baseObj, baseOff + fileLength - 8)

    // TODO partial cached index fiber
    IndexFiberCacheData(offHeapMem.fiberData, dataEnd, rootOffset)
  }
}
