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

import com.google.common.collect.ArrayListMultimap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptContext, RecordWriter}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.FromUnsafeProjection
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager
import org.apache.spark.sql.execution.datasources.oap.utils.PermutermUtils
import org.apache.spark.sql.types.StructType

private[index] class PermutermIndexRecordWriter(
    configuration: Configuration,
    writer: OutputStream,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {
  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private val multiHashMap = ArrayListMultimap.create[InternalRow, Int]()
  private var recordCount: Int = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    multiHashMap.put(v, recordCount)
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    flushToFile(writer)
    writer.close()
  }

  def flushToFile(out: OutputStream): Unit = {
    val statisticsManager = new StatisticsManager
    statisticsManager.initialize(PermutermIndexType, keySchema, configuration)

    val partitionUniqueSize = multiHashMap.keySet().size()
    val uniqueKeys = multiHashMap.keySet().toArray(new Array[InternalRow](partitionUniqueSize))
    assert(uniqueKeys.size == partitionUniqueSize)

    // build index file
    var i = 0
    var fileOffset = 0
    val offsetMap = new java.util.HashMap[InternalRow, Int]()
    fileOffset += writeHead(writer, IndexFile.INDEX_VERSION)
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
    val uniqueKeysList = new java.util.LinkedList[InternalRow]()
    import scala.collection.JavaConverters._
    uniqueKeysList.addAll(uniqueKeys.toSeq.asJava)

    val trie = InMemoryTrie()
    val trieSize = PermutermUtils.generatePermuterm(uniqueKeysList, offsetMap, trie)
    // TODO split page
    val pages: Seq[Int] = if (trieSize > 100000) {
      Seq(100000, 100000)
    } else {
      Seq(trieSize.toInt)
    }
    val treeMap = new java.util.HashMap[TrieNode, java.util.Stack[TriePointer]]()
    val treeLength = writeTrie(writer, trie, treeMap, 0)
    fileOffset += treeLength

    statisticsManager.write(writer)

    val pos = treeMap.get(trie).pop()
    IndexUtils.writeInt(writer, pos.offset)
    IndexUtils.writeInt(writer, pos.page)
    IndexUtils.writeInt(writer, dataEnd)
    IndexUtils.writeInt(writer, treeLength)
  }

  private def writeTrie(
      writer: OutputStream, trieNode: TrieNode,
      treeMap: java.util.HashMap[TrieNode, java.util.Stack[TriePointer]],
      treeOffset: Int): Int = {
    var length = 0
    trieNode.children.foreach(length += writeTrie(writer, _, treeMap, treeOffset + length))
    if (!treeMap.containsKey(trieNode)) {
      treeMap.put(trieNode, new java.util.Stack[TriePointer])
    }
    treeMap.get(trieNode).push(TriePointer(page = 1, treeOffset + length))
    IndexUtils.writeInt(writer, (trieNode.nodeKey << 16) + trieNode.childCount)
    IndexUtils.writeInt(writer, trieNode.rowIdsPointer)
    length += 8
    trieNode.children.foreach(c => {
      val pos = treeMap.get(c).pop()
      IndexUtils.writeInt(writer, pos.offset)
      IndexUtils.writeInt(writer, pos.page)
    })
    length + trieNode.childCount * 8
  }

  private def writeHead(writer: OutputStream, version: Int): Int = {
    writer.write("OAPIDX".getBytes("UTF-8"))
    assert(version <= 65535)
    val data = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(data)
    IndexFile.indexFileHeaderLength
  }
}
