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

import java.io.ByteArrayOutputStream
import java.util.Comparator

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.{IndexUtils, SpinachUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

private[spinach] case class SpinachIndexBuild(
    @transient sparkSession: SparkSession,
    indexName: String,
    indexColumns: Array[IndexColumn],
    schema: StructType,
    @transient paths: Seq[Path],
    readerClass: String,
    overwrite: Boolean = true) extends Logging {
  private lazy val ids =
    indexColumns.map(c => schema.map(_.name).toIndexedSeq.indexOf(c.columnName))
  private lazy val keySchema = StructType(ids.map(schema.toIndexedSeq(_)))
  def execute(): Seq[IndexBuildResult] = {
    if (paths.isEmpty) {
      // the input path probably be pruned, do nothing
      Nil
    } else {
      // TODO use internal scan
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val fs = paths.head.getFileSystem(hadoopConf)
      val fileIters = paths.map(fs.listFiles(_, false))
      val dataPaths = fileIters.flatMap(fileIter => new Iterator[Path] {
        override def hasNext: Boolean = fileIter.hasNext
        override def next(): Path = fileIter.next().getPath
      }.toSeq)
      val data = (if (overwrite) {
        dataPaths.filterNot(dp =>
          dp.getName.startsWith(".") || dp.getName.startsWith("_"))
      } else {
        dataPaths.filterNot(dp =>
          dp.getName.startsWith(".") || dp.getName.startsWith("_")).filterNot(
          dp => fs.exists(IndexUtils.indexFileFromDataFile(dp, indexName)))
      }).map(_.toString)
      assert(!ids.exists(id => id < 0), "Index column not exists in schema.")
      lazy val ordering = buildOrdering(ids, keySchema)
      val serializableConfiguration =
        new SerializableConfiguration(hadoopConf)
      val confBroadcast = sparkSession.sparkContext.broadcast(serializableConfiguration)
      sparkSession.sparkContext.parallelize(data, data.length).map(dataString => {
        val d = new Path(dataString)
        // TODO many task will use the same meta, so optimize here
        val meta = SpinachUtils.getMeta(confBroadcast.value.value, d.getParent) match {
          case Some(m) => m
          case None => (new DataSourceMetaBuilder).withNewDataReaderClassName(
            readerClass).withNewSchema(schema).build()
        }
        // scan every data file
        // TODO we need to set the Data Reader File class name here.
        val reader = new SpinachDataReader(d, meta, None, ids)
        val hadoopConf = confBroadcast.value.value
        val it = reader.initialize(confBroadcast.value.value)
        // key -> RowIDs list
        val hashMap = new java.util.HashMap[InternalRow, java.util.ArrayList[Long]]()
        var cnt = 0L
        while (it.hasNext) {
          val v = it.next().copy()
          if (!hashMap.containsKey(v)) {
            val list = new java.util.ArrayList[Long]()
            list.add(cnt)
            hashMap.put(v, list)
          } else {
            hashMap.get(v).add(cnt)
          }
          cnt = cnt + 1
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
        val indexFile = IndexUtils.indexFileFromDataFile(d, indexName)
        val fs = indexFile.getFileSystem(hadoopConf)
        // we are overwriting index files
        val fileOut = fs.create(indexFile, true)
        var i = 0
        var fileOffset = 0L
        val offsetMap = new java.util.HashMap[InternalRow, Long]()
        // write data segment.
        while (i < partitionUniqueSize) {
          offsetMap.put(uniqueKeys(i), fileOffset)
          val rowIds = hashMap.get(uniqueKeys(i))
          // row count for same key
          IndexUtils.writeInt(fileOut, rowIds.size())
          // 4 -> value1, stores rowIds count.
          fileOffset = fileOffset + 4
          var idIter = 0
          while (idIter < rowIds.size()) {
            IndexUtils.writeLong(fileOut, rowIds.get(idIter))
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
        writeTreeToOut(treeShape, fileOut, offsetMap, fileOffset, uniqueKeysList, keySchema, 0, -1)
        assert(uniqueKeysList.size == 1)
        IndexUtils.writeLong(fileOut, dataEnd)
        IndexUtils.writeLong(fileOut, offsetMap.get(uniqueKeysList.getFirst))
        fileOut.close()
        indexFile.toString
        IndexBuildResult(dataString, cnt, "", d.getParent.toString)
      }).collect().toSeq
    }
  }

  private def buildOrdering(
      requiredIds: Array[Int], keySchema: StructType): Ordering[InternalRow] = {
    val order = requiredIds.toSeq.map(id => SortOrder(
      BoundReference(id, keySchema(id).dataType, nullable = true),
      if (indexColumns(requiredIds.indexOf(id)).isAscending) Ascending else Descending))
    GenerateOrdering.generate(order, keySchema.toAttributes)
  }

  private def writeTreeToOut(
      tree: BTreeNode,
      out: FSDataOutputStream,
      map: java.util.HashMap[InternalRow, Long],
      fileOffset: Long,
      keysList: java.util.LinkedList[InternalRow],
      keySchema: StructType,
      listOffsetFromEnd: Int,
      nextOffset: Long): (Long, Long) = {
    var subOffset = 0L
    if (tree.children.nonEmpty) {
      // this is a non-leaf node
      // Need to write down all subtrees
      val childrenCount = tree.children.size
      assert(childrenCount == tree.root)
      var iter = childrenCount
      // write down all subtrees reversely
      var lastStart = nextOffset
      while (iter > 0) {
        iter -= 1
        val subTree = tree.children(iter)
        val subListOffsetFromEnd = listOffsetFromEnd + childrenCount - 1 - iter
        val (writeOffset, oneLevelStart) = writeTreeToOut(
          subTree, out, map, fileOffset + subOffset,
          keysList, keySchema, subListOffsetFromEnd, lastStart)
        lastStart = oneLevelStart
        subOffset += writeOffset
      }
    }
    (subOffset + writeIndexNode(
      tree, out, map, keysList, listOffsetFromEnd, nextOffset, subOffset + fileOffset), subOffset)
  }

  @transient private lazy val converter = UnsafeProjection.create(keySchema)

  /**
   * write file correspond to [[UnsafeIndexNode]]
   */
  private def writeIndexNode(
      tree: BTreeNode,
      out: FSDataOutputStream,
      map: java.util.HashMap[InternalRow, Long],
      keysList: java.util.LinkedList[InternalRow],
      listOffsetFromEnd: Int,
      nextOffset: Long,
      updateOffset: Long): Long = {
    var subOffset = 0
    // write road sign count on every node first
    IndexUtils.writeInt(out, tree.root)
    // 4 -> value3, stores tree branch count
    subOffset = subOffset + 4
    IndexUtils.writeLong(out, nextOffset)
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
      IndexUtils.writeLong(out, baseOffset + keyBuf.size)
      // assert(map.containsKey(writeList(i)))
      IndexUtils.writeLong(out, map.get(writeKey))
      // 16 -> value5, stores 2 long values, key offset and child offset
      subOffset += 16
      val writeRow = converter.apply(writeKey)
      IndexUtils.writeInt(keyBuf, writeRow.getSizeInBytes)
      writeRow.writeToStream(keyBuf, null)
      i += 1
    }
    keyBuf.writeTo(out)
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

case class IndexBuildResult(dataFile: String, rowCount: Long, fingerprint: String, parent: String)
