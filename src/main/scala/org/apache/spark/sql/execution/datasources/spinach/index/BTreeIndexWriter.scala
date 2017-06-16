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

package org.apache.spark.sql.execution.datasources.spinach.index

import java.io.ByteArrayOutputStream
import java.util.Comparator

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.InputFileNameHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.WriteResult
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.sql.execution.datasources.spinach.utils.{BTreeNode, BTreeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

// TODO respect `sparkSession.conf.get(SQLConf.PARTITION_MAX_FILES)`
private[spinach] class BTreeIndexWriter(
    relation: WriteIndexRelation,
    job: Job,
    indexColumns: Array[IndexColumn],
    keySchema: StructType,
    indexName: String,
    isAppend: Boolean) extends IndexWriter(relation, job, isAppend) {

  override def writeIndexFromRows(
      taskContext: TaskContext, iterator: Iterator[InternalRow]): Seq[IndexBuildResult] = {
    var taskReturn: Seq[IndexBuildResult] = Nil
    var writeNewFile = false
    executorSideSetup(taskContext)
    val configuration = taskAttemptContext.getConfiguration
    // to get input filename
    if (!iterator.hasNext) return Nil
    // configuration.set(DATASOURCE_OUTPUTPATH, outputPath)
    if (isAppend) {
      val fs = FileSystem.get(configuration)
      var skip = true
      var nextFile = InputFileNameHolder.getInputFileName().toString
      iterator.next()
      while(iterator.hasNext && skip) {
        val cacheFile = nextFile
        nextFile = InputFileNameHolder.getInputFileName().toString
        // avoid calling `fs.exists` for every row
        skip = cacheFile == nextFile || fs.exists(new Path(nextFile))
        iterator.next()
      }
      if (skip) return Nil
    }
    val filename = InputFileNameHolder.getInputFileName().toString
    configuration.set(IndexWriter.INPUT_FILE_NAME, filename)
    configuration.set(IndexWriter.INDEX_NAME, indexName)
    // TODO deal with partition
    // configuration.set(FileOutputFormat.OUTDIR, getWorkPath)
    var writer = newIndexOutputWriter()
    writer.initConverter(dataSchema)

    def buildOrdering(keySchema: StructType): Ordering[InternalRow] = {
      // here i change to use param id to index_id to get datatype in keySchema
      val order = keySchema.zipWithIndex.map {
        case (field, index) => SortOrder(
          BoundReference(index, field.dataType, nullable = true),
          if (indexColumns(index).isAscending) Ascending else Descending)
      }

      GenerateOrdering.generate(order, keySchema.toAttributes)
    }
    lazy val ordering = buildOrdering(keySchema)

    def commitTask(): Seq[WriteResult] = {
      try {
        var writeResults: Seq[WriteResult] = Nil
        if (writer != null) {
          writeResults = writeResults :+ writer.close()
          writer = null
        }
        super.commitTask()
        writeResults
      } catch {
        case cause: Throwable =>
          // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and
          // will cause `abortTask()` to be invoked.
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        if (writer != null) {
          writer.close()
        }
      } finally {
        super.abortTask()
      }
    }

    def writeTask(): Seq[IndexBuildResult] = {
      val statisticsManager = new StatisticsManager
      statisticsManager.initialize(BTreeIndexType, keySchema)
      // key -> RowIDs list
      val hashMap = new java.util.HashMap[InternalRow, java.util.ArrayList[Long]]()
      var cnt = 0L
      while (iterator.hasNext && !writeNewFile) {
        val fname = InputFileNameHolder.getInputFileName().toString
        if (fname != filename) {
          taskReturn = taskReturn ++: writeIndexFromRows(taskContext, iterator)
          writeNewFile = true
        } else {
          val v = InternalRow.fromSeq(iterator.next().copy().toSeq(keySchema))
          statisticsManager.addSpinachKey(v)
          if (!hashMap.containsKey(v)) {
            val list = new java.util.ArrayList[Long]()
            list.add(cnt)
            hashMap.put(v, list)
          } else {
            hashMap.get(v).add(cnt)
          }
          cnt = cnt + 1
        }
      }
      val partitionUniqueSize = hashMap.size()
      val uniqueKeys = hashMap.keySet().toArray(new Array[InternalRow](partitionUniqueSize))
      assert(uniqueKeys.size == partitionUniqueSize)
      lazy val comparator: Comparator[InternalRow] = new Comparator[InternalRow]() {
        override def compare(o1: InternalRow, o2: InternalRow): Int = {
          if (o1 == null && o2 == null) {
            0
          } else if (o1 == null) {
            -1
          } else if (o2 == null) {
            1
          } else {
            ordering.compare(o1, o2)
          }
        }
      }
      // sort keys
      java.util.Arrays.sort(uniqueKeys, comparator)
      // build index file
//      val indexFile = IndexUtils.indexFileFromDataFile(d, indexName)
//      val fs = indexFile.getFileSystem(configuration)
      // we are overwriting index files
//      val fileOut = fs.create(indexFile, true)
      var i = 0
      var fileOffset = 0L
      val offsetMap = new java.util.HashMap[InternalRow, Long]()
      // write data segment.
      while (i < partitionUniqueSize) {
        offsetMap.put(uniqueKeys(i), fileOffset)
        val rowIds = hashMap.get(uniqueKeys(i))
        // row count for same key
        IndexUtils.writeInt(writer, rowIds.size())
        // 4 -> value1, stores rowIds count.
        fileOffset = fileOffset + 4
        var idIter = 0
        while (idIter < rowIds.size()) {
          IndexUtils.writeLong(writer, rowIds.get(idIter))
          // 8 -> value2, stores a row id
          fileOffset = fileOffset + 8
          idIter = idIter + 1
        }
        i = i + 1
      }
      val dataEnd = fileOffset
      // write index segment.
      val treeShape = BTreeUtils.generate2(partitionUniqueSize)
      val uniqueKeysList = new java.util.LinkedList[InternalRow]()
      import scala.collection.JavaConverters._
      uniqueKeysList.addAll(uniqueKeys.toSeq.asJava)

      val treeOffset = writeTreeToOut(treeShape, writer, offsetMap,
        fileOffset, uniqueKeysList, keySchema, 0, -1L)

      statisticsManager.write(writer)

      assert(uniqueKeysList.size == 1)
      IndexUtils.writeLong(writer, dataEnd + treeOffset._1)
      IndexUtils.writeLong(writer, dataEnd)
      IndexUtils.writeLong(writer, offsetMap.get(uniqueKeysList.getFirst))

      taskReturn :+ IndexBuildResult(filename, cnt, "", new Path(filename).getParent.toString)
    }

    // If anything below fails, we should abort the task.
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks {
        val res = writeTask()
        commitTask()
        res
      }(catchBlock = abortTask())
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }

  /**
   * Write tree to output, return bytes written and updated nextPos
   */
  private def writeTreeToOut(
      tree: BTreeNode,
      writer: IndexOutputWriter,
      map: java.util.HashMap[InternalRow, Long],
      fileOffset: Long,
      keysList: java.util.LinkedList[InternalRow],
      keySchema: StructType,
      listOffsetFromEnd: Int,
      nextP: Long): (Long, Long) = {
    if (tree.children.nonEmpty) {
      var subOffset = 0L
      // this is a non-leaf node
      // Need to write down all subtrees
      val childrenCount = tree.children.size
      assert(childrenCount == tree.root)
      var iter = childrenCount
      var currentNextPos = nextP
      // write down all subtrees reversely
      while (iter > 0) {
        iter -= 1
        val subTree = tree.children(iter)
        val subListOffsetFromEnd = listOffsetFromEnd + childrenCount - 1 - iter
        val (writeOffset, newNext) = writeTreeToOut(
          subTree, writer, map, fileOffset + subOffset,
          keysList, keySchema, subListOffsetFromEnd, currentNextPos)
        currentNextPos = newNext
        subOffset += writeOffset
      }
      (subOffset + writeIndexNode(
        tree, writer, map, keysList, listOffsetFromEnd, subOffset + fileOffset, -1L
      ), currentNextPos)
    } else {
      (writeIndexNode(
        tree, writer, map, keysList, listOffsetFromEnd, fileOffset, nextP), fileOffset)
    }
  }

  @transient private lazy val projector = UnsafeProjection.create(keySchema)

  /**
   * write file correspond to [[UnsafeIndexNode]]
   */
  private def writeIndexNode(
                              tree: BTreeNode,
                              writer: IndexOutputWriter,
                              map: java.util.HashMap[InternalRow, Long],
                              keysList: java.util.LinkedList[InternalRow],
                              listOffsetFromEnd: Int,
                              updateOffset: Long,
                              nextPointer: Long): Long = {
    var subOffset = 0
    // write road sign count on every node first
    IndexUtils.writeInt(writer, tree.root)
    // 4 -> value3, stores tree branch count
    subOffset = subOffset + 4
    IndexUtils.writeLong(writer, nextPointer)
    // 8 -> value4, stores next offset
    subOffset = subOffset + 8
    // For all IndexNode, write down all road sign, each pointing to specific data segment
    val start = keysList.size - listOffsetFromEnd - tree.root
    val end = keysList.size - listOffsetFromEnd
    val writeList = keysList.subList(start, end).asScala
    val keyBuf = new ByteArrayOutputStream()
    // offset0 pointer0, offset1 pointer1, ..., offset(n-1) pointer(n-1),
    // len0 key0, len1 key1, ..., len(n-1) key(n-1)
    // 16 <- value5
    val baseOffset = updateOffset + subOffset + tree.root * 16
    var i = 0
    while (i < tree.root) {
      val writeKey = writeList(i)
      IndexUtils.writeLong(writer, baseOffset + keyBuf.size)
      // assert(map.containsKey(writeList(i)))
      IndexUtils.writeLong(writer, map.get(writeKey))
      // 16 -> value5, stores 2 long values, key offset and child offset
      subOffset += 16
      val writeRow = projector.apply(writeKey)
      IndexUtils.writeInt(keyBuf, writeRow.getSizeInBytes)
      writeRow.writeToStream(keyBuf, null)
      i += 1
    }
    writer.write(keyBuf.toByteArray)
    subOffset += keyBuf.size
    map.put(writeList.head, updateOffset)
    var rmCount = tree.root
    while (rmCount > 1) {
      rmCount -= 1
      keysList.remove(keysList.size - listOffsetFromEnd - rmCount)
    }
    subOffset
  }
}
