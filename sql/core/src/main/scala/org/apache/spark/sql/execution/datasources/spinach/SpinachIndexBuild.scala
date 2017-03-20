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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.Comparator

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.{IndexUtils, SpinachUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

private[spinach] case class SpinachIndexBuild(
    @transient sparkSession: SparkSession,
    indexName: String,
    indexColumns: Array[IndexColumn],
    schema: StructType,
    @transient paths: Seq[Path],
    readerClass: String,
    indexType: AnyIndexType = BTreeIndexType,
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
      lazy val ordering = buildOrdering(keySchema)
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

        indexType match {
          case BTreeIndexType =>
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
            writeTreeToOut(treeShape, fileOut, offsetMap,
              fileOffset, uniqueKeysList, keySchema, 0, -1L)
            assert(uniqueKeysList.size == 1)
            IndexUtils.writeLong(fileOut, dataEnd)
            IndexUtils.writeLong(fileOut, offsetMap.get(uniqueKeysList.getFirst))
            fileOut.close()
            indexFile.toString
            IndexBuildResult(d.getName, cnt, "", d.getParent.toString)
          case BloomFilterIndexType =>
            val bf_index = new BloomFilter()
            var elemCnt = 0 // element count
          val boundReference = keySchema.zipWithIndex.map(x =>
            BoundReference(x._2, x._1.dataType, nullable = true))
            // for multi-column index, add all subsets into bloom filter
            // For example, a column with a = 1, b = 2, a and b are index columns
            // then three records: a = 1, b = 2, a = 1 b = 2, are inserted to bf
            val projector = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
              UnsafeProjection.create(s.toArray)).toArray
            while (it.hasNext) {
              val row = it.next()
              elemCnt += 1
              projector.foreach(p => bf_index.addValue(p(row).getBytes))
            }
            val indexFile = IndexUtils.indexFileFromDataFile(d, indexName)
            val fs = indexFile.getFileSystem(hadoopConf)
            // Bloom filter index file format:
            // numOfLong            4 Bytes, Int, record the total number of Longs in bit array
            // numOfHashFunction    4 Bytes, Int, record the total number of Hash Functions
            // elementCount         4 Bytes, Int, number of elements stored in the related DataFile
            //
            // long 1               8 Bytes, Long, the first element in bit array
            // long 2               8 Bytes, Long, the second element in bit array
            // ...
            // long $numOfLong      8 Bytes, Long, the $numOfLong -th element in bit array
            //
            // dataEndOffset        8 Bytes, Long, data end offset
            // rootOffset           8 Bytes, Long, root Offset
            val fileOut = fs.create(indexFile, true) // overwrite index file
            val bitArray = bf_index.getBitMapLongArray
            val numHashFunc = bf_index.getNumOfHashFunc
            IndexUtils.writeInt(fileOut, bitArray.length)
            IndexUtils.writeInt(fileOut, numHashFunc)
            IndexUtils.writeInt(fileOut, elemCnt)
            var offset = 12L
            bitArray.foreach(l => {
              IndexUtils.writeLong(fileOut, l)
              offset += 8
            })
            IndexUtils.writeLong(fileOut, offset) // dataEnd
            IndexUtils.writeLong(fileOut, offset) // rootOffset
            fileOut.close()
            IndexBuildResult(d.getName, elemCnt, "", d.getParent.toString)
          case BitMapIndexType =>
            // Current impl just for fast proving the effect of BitMap Index,
            // we can do the optimize below:
            // 1. each bitset in hashmap value has same length, we can save the
            //    hash map in raw bits in file, like B+ Index above
            // 2. use the BitMap with bit compress like javaewah
            // TODO: BitMap Index storage format optimize
            // get the tmpMap and total rowCnt in first travers
            val tmpMap = new mutable.HashMap[InternalRow, mutable.ListBuffer[Int]]()
            var rowCnt = 0
            while (it.hasNext) {
              val v = it.next().copy()
              if (!tmpMap.contains(v)) {
                val list = new mutable.ListBuffer[Int]()
                list += rowCnt
                tmpMap.put(v, list)
              } else {
                tmpMap.get(v).get += rowCnt
              }
              rowCnt += 1
            }
            // generate the bitset hashmap
            val hashMap = new mutable.HashMap[InternalRow, BitSet]()
            tmpMap.foreach(kv => {
              val bs = new BitSet(rowCnt)
              kv._2.foreach(bs.set)
              hashMap.put(kv._1, bs)
            })
            // get index file handler
            val indexFile = IndexUtils.indexFileFromDataFile(d, indexName)
            val fs = indexFile.getFileSystem(hadoopConf)
            val fileOut = fs.create(indexFile, true)
            // serialize hashMap and get length
            val writeBuf = new ByteArrayOutputStream()
            val out = new ObjectOutputStream(writeBuf)
            out.writeObject(hashMap)
            out.flush()
            val objLen = writeBuf.size()
            // write byteArray length and byteArray
            IndexUtils.writeInt(fileOut, objLen)
            fileOut.write(writeBuf.toByteArray)
            // write dataEnd
            IndexUtils.writeLong(fileOut, 4 + objLen)
            out.close()
            fileOut.close()
            IndexBuildResult(d.getName, rowCnt, "", d.getParent.toString)
          case _ => throw new Exception("unsupported index type")
        }
      }).collect().toSeq
    }
  }

  private def buildOrdering(keySchema: StructType): Ordering[InternalRow] = {
    // here i change to use param id to index_id to get datatype in keySchema
    val order = keySchema.zipWithIndex.map {
      case (field, index) => SortOrder(
        BoundReference(index, field.dataType, nullable = true),
        if (indexColumns(index).isAscending) Ascending else Descending)
    }

    GenerateOrdering.generate(order, keySchema.toAttributes)
  }

  /**
   * Write tree to output, return bytes written and updated nextPos
   */
  private def writeTreeToOut(
      tree: BTreeNode,
      out: FSDataOutputStream,
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
          subTree, out, map, fileOffset + subOffset,
          keysList, keySchema, subListOffsetFromEnd, currentNextPos)
        currentNextPos = newNext
        subOffset += writeOffset
      }
      (subOffset + writeIndexNode(
        tree, out, map, keysList, listOffsetFromEnd, subOffset + fileOffset, -1L), currentNextPos)
    } else {
      (writeIndexNode(tree, out, map, keysList, listOffsetFromEnd, fileOffset, nextP), fileOffset)
    }
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
      updateOffset: Long,
      nextPointer: Long): Long = {
    var subOffset = 0
    // write road sign count on every node first
    IndexUtils.writeInt(out, tree.root)
    // 4 -> value3, stores tree branch count
    subOffset = subOffset + 4
    IndexUtils.writeLong(out, nextPointer)
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
