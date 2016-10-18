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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.SerializableConfiguration

private[spinach] case class SpinachIndexBuild(
    sqlContext: SQLContext,
    indexName: String,
    indexColumns: Array[IndexColumn],
    schema: StructType,
    paths: Array[String]) extends Logging {
  @transient private lazy val ids =
    indexColumns.map(c => schema.map(_.name).toIndexedSeq.indexOf(c.columnName))
  @transient private lazy val keySchema = StructType(ids.map(schema.toIndexedSeq(_)))
  def execute(): RDD[InternalRow] = {
    if (paths.isEmpty) {
      // the input path probably be pruned, do nothing
    } else {
      // TODO use internal scan
      val path = paths(0)
      @transient val p = new Path(path)
      @transient val fs = p.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      @transient val fileIter = fs.listFiles(p, true)
      @transient val dataPaths = new Iterator[Path] {
        override def hasNext: Boolean = fileIter.hasNext
        override def next(): Path = fileIter.next().getPath
      }.toSeq
      val data = dataPaths.map(_.toString).filter(
        _.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))
      assert(!ids.exists(id => id < 0), "Index column not exists in schema.")
      @transient lazy val ordering = buildOrdering(ids, keySchema)
      val serializableConfiguration =
        new SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration)
      val confBroadcast = sqlContext.sparkContext.broadcast(serializableConfiguration)
      val num = dataPaths.length
      sqlContext.sparkContext.parallelize(data, num).map(dataString => {
      // data.foreach(dataString => {
        val d = new Path(dataString)
        // scan every data file
        val reader = new SpinachDataReader(d, schema, None, ids)
        val hadoopConf = confBroadcast.value.value
        val it = reader.initialize(confBroadcast.value.value)
        // TODO maybe use Long as RowId?
        val hashMap = new java.util.HashMap[InternalRow, java.util.ArrayList[Int]]()
        var cnt = 0
        while (it.hasNext) {
          val v = it.next().copy()
          if (!hashMap.containsKey(v)) {
            val list = new java.util.ArrayList[Int]()
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
        val dataFilePathString = d.toString
        val pos = dataFilePathString.lastIndexOf(SpinachFileFormat.SPINACH_DATA_EXTENSION)
        val indexFile = new Path(dataFilePathString.substring(
          0, pos) + "." + indexName + SpinachFileFormat.SPINACH_INDEX_EXTENSION)
        val fs = indexFile.getFileSystem(hadoopConf)
        val fileOut = fs.create(indexFile, false)
        var i = 0
        var fileOffset = 0
        val offsetMap = new java.util.HashMap[InternalRow, Int]()
        // write data segment.
        while (i < partitionUniqueSize) {
          offsetMap.put(uniqueKeys(i), fileOffset)
          val rowIds = hashMap.get(uniqueKeys(i))
          // row count for same key
          IndexUtils.writeInt(fileOut, rowIds.size())
          fileOffset = fileOffset + 4
          var idIter = 0
          while (idIter < rowIds.size()) {
            IndexUtils.writeInt(fileOut, rowIds.get(idIter))
            fileOffset = fileOffset + 4
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
        IndexUtils.writeInt(fileOut, dataEnd)
        IndexUtils.writeInt(fileOut, offsetMap.get(uniqueKeysList.getFirst))
        fileOut.close()
        indexFile.toString
      }).collect()
    }
    sqlContext.sparkContext.emptyRDD[InternalRow]
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
      map: java.util.HashMap[InternalRow, Int],
      fileOffset: Int,
      keysList: java.util.LinkedList[InternalRow],
      keySchema: StructType,
      listOffsetFromEnd: Int,
      nextOffset: Int): (Int, Int) = {
    var subOffset = 0
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
    (subOffset + writeIndexNode2(
      tree, out, map, keysList, listOffsetFromEnd, nextOffset, subOffset + fileOffset), subOffset)
  }

  @transient private lazy val converter = UnsafeProjection.create(keySchema)

  /**
   * write file correspond to UnsafeIndexNode22
   */
  private def writeIndexNode2(
      tree: BTreeNode,
      out: FSDataOutputStream,
      map: java.util.HashMap[InternalRow, Int],
      keysList: java.util.LinkedList[InternalRow],
      listOffsetFromEnd: Int,
      nextOffset: Int,
      updateOffset: Int): Int = {
    var subOffset = 0
    // write road sign count on every node first
    IndexUtils.writeInt(out, tree.root)
    subOffset = subOffset + 4
    IndexUtils.writeInt(out, nextOffset)
    subOffset = subOffset + 4
    // For all IndexNode, write down all road sign, each pointing to specific data segment
    val start = keysList.size - listOffsetFromEnd - tree.root
    val end = keysList.size - listOffsetFromEnd
    val writeList = keysList.subList(start, end).asScala
    val keyBuf = new ByteArrayOutputStream()
    // offset0 pointer0, offset1 pointer1, ..., offset(n-1) pointer(n-1),
    // len0 key0, len1 key1, ..., len(n-1) key(n-1)
    val baseOffset = updateOffset + subOffset + tree.root * 8
    var i = 0
    while (i < tree.root) {
      val writeKey = writeList(i)
      IndexUtils.writeInt(out, baseOffset + keyBuf.size)
      // assert(map.containsKey(writeList(i)))
      IndexUtils.writeInt(out, map.get(writeKey))
      subOffset += 8
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
