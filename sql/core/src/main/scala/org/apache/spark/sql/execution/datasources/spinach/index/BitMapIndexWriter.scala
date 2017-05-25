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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.InputFileNameHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.WriteResult
import org.apache.spark.sql.execution.datasources.spinach.io.DataFile
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

// TODO respect `sparkSession.conf.get(SQLConf.PARTITION_MAX_FILES)`
private[spinach] class BitMapIndexWriter(
    relation: WriteIndexRelation,
    job: Job,
    indexColumns: Array[IndexColumn],
    keySchema: StructType,
    indexName: String,
    isAppend: Boolean) extends IndexWriter(relation, job, isAppend) {
  private var encodedSchema: StructType = _

  override def writeIndexFromRows(
      taskContext: TaskContext, iterator: Iterator[InternalRow]): Seq[IndexBuildResult] = {
    var taskReturn: Seq[IndexBuildResult] = Nil
    var writeNewFile = false
    executorSideSetup(taskContext)
    val configuration = taskAttemptContext.getConfiguration
    // to get input filename
    if (!iterator.hasNext) return Nil
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
    var writer = newIndexOutputWriter()
    writer.initConverter(dataSchema)
    val dataFile = DataFile(filename, dataFileSchema, readerClassName)
    val dictionaries = keySchema.map(field => dataFileSchema.indexOf(field))
      .map(ordinal => dataFile.getDictionary(ordinal, configuration))
      .toArray
    encodedSchema = DataFile.encodeSchema(dictionaries, keySchema)
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
      statisticsManager.initialize(BitMapIndexType, encodedSchema)
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
          taskReturn = taskReturn ++: writeIndexFromRows(taskContext, iterator)
          writeNewFile = true
        } else {
          val v = DataFile.encodeKey(dictionaries, keySchema, iterator.next().copy())
          statisticsManager.addSpinachKey(v)
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
      // generate the bitset hashmap
      val hashMap = new mutable.HashMap[InternalRow, BitSet]()
      tmpMap.foreach(kv => {
        val bs = new BitSet(rowCnt)
        kv._2.foreach(bs.set)
        hashMap.put(kv._1, bs)
      })
      // serialize hashMap and get length
      val writeBuf = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(writeBuf)
      out.writeObject(hashMap)
      out.flush()
      val objLen = writeBuf.size()
      // write byteArray length and byteArray
      IndexUtils.writeInt(writer, objLen)
      writer.write(writeBuf.toByteArray)
      out.close()
      val indexEnd = 4 + objLen
      var offset: Long = indexEnd

      statisticsManager.write(writer)

      // write index file footer
      IndexUtils.writeLong(writer, indexEnd) // statistics start pos
      IndexUtils.writeLong(writer, offset) // index file end offset
      IndexUtils.writeLong(writer, indexEnd) // dataEnd


      // writer.close()
      taskReturn :+ IndexBuildResult(filename, rowCnt, "", new Path(filename).getParent.toString)
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
}
