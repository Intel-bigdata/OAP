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

import java.io.{ByteArrayInputStream, ObjectInputStream}

import org.apache.spark.sql.types.StructType

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.io.ChunkedByteBuffer

private[oap] case class TrieScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = false

  var indexFiber: IndexFiber = _
  var indexData: CacheResult = _
  var matchRoot: TrieNode = null
  private lazy val allPointers = matchRoot.allPointers
  private lazy val allUnsafeIds = allPointers.map(UnsafeIds(indexData.buffer, _))
  private lazy val internalIter = allUnsafeIds.iterator
  private var remain: Int = 0
  private var currentCount: Int = 0
  private var readingUnsafeIds: UnsafeIds = _

  def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    logDebug("Loading Index File: " + path)
    logDebug("\tFile Size: " + path.getFileSystem(conf).getFileStatus(path).getLen)
    val indexFile = IndexFile(path)
    indexFiber = IndexFiber(indexFile)
    indexData = FiberCacheManager.getOrElseUpdate(indexFiber, conf)
    val root = open(indexData.buffer, keySchema, indexFile.version(conf))

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

  def open(
    data: ChunkedByteBuffer,
    keySchema: StructType,
    version: Int = IndexFile.INDEX_VERSION): TrieNode = {
    assert(version == IndexFile.INDEX_VERSION, "Unsupported version of index data!")
    val (baseObject, baseOffset): (Object, Long) = data.chunks.head match {
      case buf: DirectBuffer => (null, buf.address())
      case _ => (data.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    val dataEnd = Platform.getInt(baseObject, baseOffset + data.size - 8)
    val length = Platform.getInt(baseObject, baseOffset + data.size - 4)
    val rootPage = Platform.getInt(baseObject, baseOffset + data.size - 12)
    val rootOffset = Platform.getInt(baseObject, baseOffset + data.size - 16)
    UnsafeTrie(data, rootPage, rootOffset, dataEnd)
  }

  override def toString: String = "TrieScanner"

  override def hasNext: Boolean = if (matchRoot != null) {
    internalIter.hasNext || remain > 0
  } else {
    false
  }

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
