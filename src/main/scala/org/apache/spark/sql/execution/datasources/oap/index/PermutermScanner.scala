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
import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer

private[oap] case class PermutermScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = false

  var matchRoot: TrieNode = null
  var permutermFootCache: CacheResult = _
  var permutermDataCache: CacheResult = _
  private var permutermFile: PermutermIndexFile = _
  protected lazy val allPointers = matchRoot.allPointers
  private lazy val allUnsafeIds = allPointers.map(UnsafeIds(permutermDataCache.buffer, _))
  private lazy val internalIter = allUnsafeIds.iterator
  private var remain: Int = 0
  private var currentCount: Int = 0
  private var readingUnsafeIds: UnsafeIds = _

  def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    logDebug("Loading Index File: " + path)
    logDebug("\tFile Size: " + path.getFileSystem(conf).getFileStatus(path).getLen)
    permutermFile = PermutermIndexFile(path)
    val permutermFootFiber = PermutermFootFiber(permutermFile)
    permutermFootCache = FiberCacheManager.getOrElseUpdate(permutermFootFiber, conf)
    val root = open(permutermFootCache.buffer, conf, permutermFile.version(conf))

    _init(root)
    this
  }

  def _init(root: TrieNode): Unit = traverse(root, 0)
  def traverse(trieNode: TrieNode, matching: Int): Unit = {
    if (pattern.length == matching) {
      matchRoot = trieNode
    } else {
      trieNode.children.find(_.nodeKey == pattern(matching)) match {
        case None => matchRoot = null
        case Some(node) => traverse(node, matching + 1)
      }
    }
  }

  def getPageData(page: TriePage, conf: Configuration): ChunkedByteBuffer = {
    val permutermPage = PermutermFiber(permutermFile, page.offset, page.length)
    val permutermPageCache = FiberCacheManager.getOrElseUpdate(permutermPage, conf)
    permutermPageCache.buffer
  }

  def open(
      data: ChunkedByteBuffer,
      conf: Configuration,
      version: Int = IndexFile.INDEX_VERSION): TrieNode = {
    assert(version == IndexFile.INDEX_VERSION, "Unsupported version of index data!")
    val (baseObject, baseOffset): (Object, Long) = data.chunks.head match {
      case buf: DirectBuffer => (null, buf.address())
      case _ => (data.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    val dataEnd = Platform.getInt(baseObject, baseOffset + data.size - 8)
    val rootPage = Platform.getInt(baseObject, baseOffset + data.size - 12)
    val rootOffset = Platform.getInt(baseObject, baseOffset + data.size - 16)
    permutermDataCache = FiberCacheManager.getOrElseUpdate(
      PermutermFiber(permutermFile, 0, dataEnd), conf)
    val pageTable = UnsafeTrieFooter(data)
    UnsafeTrie(
      data, rootPage, rootOffset, dataEnd, i => getPageData(pageTable.page(i, dataEnd), conf))
  }

  override def toString: String = "PermutermScanner"

  override def hasNext: Boolean = matchRoot != null && (remain > 0 || internalIter.hasNext)

  override def next(): Long = {
    if (internalIter.hasNext && remain == 0) {
      readingUnsafeIds = internalIter.next()
      currentCount = readingUnsafeIds.count
      remain = currentCount
      assert(remain > 0)
    }
    if (remain == 0) {
      0
    } else {
      val ret = readingUnsafeIds(currentCount - remain)
      remain -= 1
      ret
    }
  }
}
