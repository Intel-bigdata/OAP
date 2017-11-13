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

import java.io.OutputStream

import scala.collection.mutable

import com.google.common.collect.ArrayListMultimap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptContext, RecordWriter}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.FromUnsafeProjection
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager
import org.apache.spark.sql.execution.datasources.oap.utils.PermutermUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

private[index] class PermutermIndexRecordWriter(
    configuration: Configuration,
    writer: OutputStream,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {
  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private val multiHashMap = ArrayListMultimap.create[UTF8String, Int]()
  private var recordCount: Int = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    multiHashMap.put(v.getUTF8String(0), recordCount)
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    flushToFile()
    writer.close()
  }

  def flushToFile(): Unit = {
    val statisticsManager = new StatisticsManager
    statisticsManager.initialize(PermutermIndexType, keySchema, configuration)

    val partitionUniqueSize = multiHashMap.keySet().size()
    val uniqueKeys = multiHashMap.keySet().toArray(
      new Array[UTF8String](partitionUniqueSize)).sorted
    assert(uniqueKeys.length == partitionUniqueSize)

    // build index file
    var i = 0
    var fileOffset = 0
    val offsetMap = new java.util.HashMap[UTF8String, Int]()
    fileOffset += IndexUtils.writeHead(writer, IndexFile.INDEX_VERSION)
    // write data segment.
    while (i < partitionUniqueSize) {
      offsetMap.put(uniqueKeys(i), fileOffset)
      val rowIds = multiHashMap.get(uniqueKeys(i))
      // row count for same key
      IndexUtils.writeInt(writer, rowIds.size())
      // 4 -> value1, stores rowIds count.
      fileOffset += 4
      var idIter = 0
      while (idIter < rowIds.size()) {
        // TODO wrap this to define use Long or Int
        IndexUtils.writeInt(writer, rowIds.get(idIter))
        fileOffset += 4
        idIter += 1
      }
      i += 1
    }
    val dataEnd = fileOffset
    // write index segment.
    val uniqueKeysList = new java.util.LinkedList[UTF8String]()
    import scala.collection.JavaConverters._
    uniqueKeysList.addAll(uniqueKeys.toSeq.asJava)

    val trie = InMemoryTrie()
    val trieSize = PermutermUtils.generatePermuterm(uniqueKeysList, offsetMap, trie)
    val maxPage = configuration.getInt(SQLConf.OAP_PERMUTERM_MAX_GROUP_SIZE.key, 128 * 1024)
    val pages: Seq[Int] = PermutermUtils.generatePages(trieSize + 1, maxPage)
    val treeMap = new java.util.HashMap[TrieNode, java.util.Stack[TriePointer]]()
    val pagedWriter = new PagedWriter(writer, pages)
    pagedWriter.initialize()
    // from page #1 to page #{pages.size}
    writeTrie(pagedWriter, trie, treeMap)

    // in page #{pages.size + 1}.
    // This is last page of file, will not appear in page table
    pagedWriter.writePageTable()
    pagedWriter.writeStatistics(statisticsManager)

    val pos = treeMap.get(trie).pop()
    pagedWriter.writeFooter(pos, dataEnd)
  }

  private def writeTrie(
      writer: PagedWriter, trieNode: TrieNode,
      treeMap: java.util.HashMap[TrieNode, java.util.Stack[TriePointer]]): Int = {
    var length = 0
    trieNode.children.foreach(length += writeTrie(writer, _, treeMap))
    length + writer.writeNode(trieNode, treeMap)
  }
}

private class PagedWriter(internalWriter: OutputStream, pageSplit: Seq[Int]) {
  private var currentNodeInPage: Int = _
  private var currentLengthInPage: Int = _
  private var currentPage: Int = _
  private var currentPageStart: Long = _
  private val pageMap: mutable.Map[Int, TriePage] = mutable.Map[Int, TriePage]()
  def initialize(): Unit = {
    currentPageStart = 0
    currentNodeInPage = 0
    currentLengthInPage = 0
    currentPage = 1
  }
  private def switchToNewPageHeadIfNecessary(): Unit = {
    if (currentNodeInPage >= pageSplit(currentPage - 1)) {
      finishPage()
      currentPage += 1
      currentNodeInPage = 0
      currentPageStart += currentLengthInPage
      currentLengthInPage = 0
    }
  }
  private def finishPage(): Unit = {
    pageMap += (currentPage -> TriePage(currentPageStart, currentLengthInPage))
  }
  def writePageTable(): Unit = {
    IndexUtils.writeInt(internalWriter, pageSplit.size)
    (1 to pageSplit.size).foreach(i => {
      IndexUtils.writeLong(internalWriter, pageMap(i).offset)
      IndexUtils.writeInt(internalWriter, pageMap(i).length)
    })
    currentLengthInPage = currentLengthInPage + 4 + 12 * pageSplit.size
  }
  def writeStatistics(statisticsManager: StatisticsManager): Unit = {
    currentLengthInPage += statisticsManager.write(internalWriter).toInt
  }
  def writeNode(
      trieNode: TrieNode,
      treeMap: java.util.HashMap[TrieNode, java.util.Stack[TriePointer]]): Int = {
    IndexUtils.writeInt(internalWriter, (trieNode.nodeKey << 16) + trieNode.childCount)
    IndexUtils.writeInt(internalWriter, trieNode.rowIdsPointer)
    trieNode.children.foreach(c => {
      val pos = treeMap.get(c).pop()
      IndexUtils.writeInt(internalWriter, pos.offset)
      IndexUtils.writeInt(internalWriter, pos.page)
    })
    // push after pop children
    if (!treeMap.containsKey(trieNode)) {
      treeMap.put(trieNode, new java.util.Stack[TriePointer])
    }
    treeMap.get(trieNode).push(TriePointer(currentPage, currentLengthInPage))
    val nodeLength = 8 + trieNode.childCount * 8
    currentLengthInPage = currentLengthInPage + nodeLength
    currentNodeInPage += 1
    switchToNewPageHeadIfNecessary()
    nodeLength
  }
  def writeFooter(root: TriePointer, dataEnd: Int): Unit = {
    IndexUtils.writeInt(internalWriter, root.offset)
    IndexUtils.writeInt(internalWriter, root.page)
    IndexUtils.writeInt(internalWriter, dataEnd)
    currentLengthInPage += 16
    IndexUtils.writeInt(internalWriter, currentLengthInPage)
  }
}

case class TriePage(offset: Long, length: Int)
