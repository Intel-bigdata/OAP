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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.rdd.InputFileNameHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.expressions.FromUnsafeProjection
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

// TODO respect `sparkSession.conf.get(SQLConf.PARTITION_MAX_FILES)`
private[oap] class BitMapIndexWriter(
    indexColumns: Array[IndexColumn],
    keySchema: StructType,
    indexName: String,
    time: String,
    isAppend: Boolean) extends IndexWriter {

  override def writeIndexFromRows(description: WriteJobDescription,
      writer: IndexOutputWriter, iterator: Iterator[InternalRow]): Seq[IndexBuildResult] = {
    var taskReturn: Seq[IndexBuildResult] = Nil
    var writeNewFile = false
    val configuration = description.serializableHadoopConf.value
    // to get input filename
    if (!iterator.hasNext) return Nil

    if (isAppend) {
      def isIndexExists(fileName: String): Boolean = {
        val indexPath =
          IndexUtils.indexFileFromDataFile(new Path(fileName), indexName, time)
        val fs = FileSystem.get(configuration)
        fs.exists(indexPath)
      }

      var nextFile = InputFileNameHolder.getInputFileName().toString
      if(nextFile== null ||  nextFile.isEmpty) return Nil
      var skip = isIndexExists(nextFile)

      while(iterator.hasNext && skip) {
        val cacheFile = nextFile
        nextFile = InputFileNameHolder.getInputFileName().toString
        // avoid calling `fs.exists` for every row
        skip = cacheFile == nextFile || isIndexExists(nextFile)
        if(skip) iterator.next()
      }
      if (skip) return Nil
    }

    val filename = InputFileNameHolder.getInputFileName().toString
    writer.initIndexInfo(filename, indexName, time)

    def writeTask(): Seq[IndexBuildResult] = {
      val statisticsManager = new StatisticsManager
      statisticsManager.initialize(BitMapIndexType, keySchema, configuration)
      // Current impl just for fast proving the effect of BitMap Index,
      // we can do the optimize below:
      // 1. each bitset in hashmap value has same length, we can save the
      //    hash map in raw bits in file, like B+ Index above
      // 2. use the BitMap with bit compress like javaewah
      // TODO: BitMap Index storage format optimize
      // get the tmpMap and total rowCnt in first travers
      val tmpMap = new mutable.HashMap[InternalRow, mutable.ListBuffer[Int]]()
      var rowCnt = 0
      while (iterator.hasNext && !writeNewFile) {
        val fname = InputFileNameHolder.getInputFileName().toString
        if (fname != filename) {
          taskReturn = taskReturn ++: writeIndexFromRows(description, writer.copy(), iterator)
          writeNewFile = true
        } else {
          val v = genericProjector(iterator.next()).copy()
          statisticsManager.addOapKey(v)
          if (!tmpMap.contains(v)) {
            val list = new mutable.ListBuffer[Int]()
            list += rowCnt
            tmpMap.put(v, list)
          } else {
            tmpMap.get(v).get += rowCnt
          }
          rowCnt += 1
        }
      }
      val ordering = GenerateOrdering.create(keySchema)
      val sortedKeyList = tmpMap.keySet.toList.sorted(ordering)
      val header = writeHead(writer, IndexFile.INDEX_VERSION)
      // Serialize sortedKeyList and get length
      val writeSortedKeyListBuf = new ByteArrayOutputStream()
      val sortedKeyListOut = new ObjectOutputStream(writeSortedKeyListBuf)
      sortedKeyListOut.writeObject(sortedKeyList)
      sortedKeyListOut.flush()
      val sortedKeyListObjLen = writeSortedKeyListBuf.size()
      // Write sortedKeyList byteArray length and byteArray
      IndexUtils.writeInt(writer, sortedKeyListObjLen)
      writer.write(writeSortedKeyListBuf.toByteArray)
      sortedKeyListOut.close()

      // Generate the fixed size bitset
      val bs = new BitSet(rowCnt)
      val firstKey = sortedKeyList.head
      tmpMap.get(firstKey).get.foreach(bs.set)
      val firstBitMapBuf = new ByteArrayOutputStream()
      val firstBitMapOut = new ObjectOutputStream(firstBitMapBuf)
      firstBitMapOut.writeObject(bs)
      val elementBitMapSize = firstBitMapBuf.size
      firstBitMapOut.close()
      // Write the single bitmap size used by partial loading during index scanning.
      IndexUtils.writeInt(writer, elementBitMapSize)

      // Serialize and write each BitMap
      sortedKeyList.foreach(sortedKey => {
        tmpMap.get(sortedKey).get.foreach(bs.set)
        val bsWriteBitMapBuf = new ByteArrayOutputStream()
        val bsBitMapOut = new ObjectOutputStream(bsWriteBitMapBuf)
        bsBitMapOut.writeObject(bs)
        bsBitMapOut.flush()
        writer.write(bsWriteBitMapBuf.toByteArray)
        bsBitMapOut.close()
      })
      val bitmapCount = sortedKeyList.length
      val totalBitMapLength = bitmapCount * elementBitMapSize
      // The first 4 is for sortedKeyListObjLen value.
      // The second 4 is for elementBitMapSize value.
      val indexEnd = header + 4 + sortedKeyListObjLen + 4 + totalBitMapLength
      var offset: Long = indexEnd

      statisticsManager.write(writer)

      // write index file footer
      IndexUtils.writeLong(writer, indexEnd) // statistics start pos
      IndexUtils.writeLong(writer, offset) // index file end offset
      IndexUtils.writeLong(writer, indexEnd) // dataEnd

      // avoid fd leak
      writer.close()

      taskReturn :+ IndexBuildResult(new Path(filename).getName, rowCnt, "",
        new Path(filename).getParent.toString)
    }

    writeTask()
  }
  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)
}
