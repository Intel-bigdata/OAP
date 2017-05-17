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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.InputFileNameHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.WriteResult
import org.apache.spark.sql.execution.datasources.spinach.io.DataFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

// TODO respect `sparkSession.conf.get(SQLConf.PARTITION_MAX_FILES)`
private[spinach] class BloomFilterIndexWriter(
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
    val dataFile = DataFile(filename, dataFileSchema, readerClassName)
    val dictionaries = keySchema.map(field => dataFileSchema.indexOf(field))
      .map(ordinal => dataFile.getDictionary(ordinal, configuration))
      .toArray
    val encodedSchema = DataFile.encodeSchema(dictionaries, keySchema)
    // TODO deal with partition
    var writer = newIndexOutputWriter()
    writer.initConverter(dataSchema)

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
      val bfMaxBits = configuration.getInt(
        SQLConf.SPINACH_BLOOMFILTER_MAXBITS.key, 1073741824) // default 1 << 30
      val bfNumOfHashFunc = configuration.getInt(
        SQLConf.SPINACH_BLOOMFILTER_NUMHASHFUNC.key, 3)
      logDebug("Building bloom with paratemeter: maxBits = "
        + bfMaxBits + " numHashFunc = " + bfNumOfHashFunc)
      val bfIndex = new BloomFilter(bfMaxBits, bfNumOfHashFunc)()
      var elemCnt = 0 // element count
      val boundReference = encodedSchema.zipWithIndex.map(x =>
        BoundReference(x._2, x._1.dataType, nullable = true))
      // for multi-column index, add all subsets into bloom filter
      // For example, a column with a = 1, b = 2, a and b are index columns
      // then three records: a = 1, b = 2, a = 1 b = 2, are inserted to bf
      val projector = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
        UnsafeProjection.create(s.toArray)).toArray
      while (iterator.hasNext && !writeNewFile) {
        val fname = InputFileNameHolder.getInputFileName().toString
        if (fname != filename) {
          taskReturn = taskReturn ++: writeIndexFromRows(taskContext, iterator)
          writeNewFile = true
        } else {
          val row = DataFile.encodeKey(dictionaries, keySchema, iterator.next().copy())
          elemCnt += 1
          projector.foreach(p => bfIndex.addValue(p(row).getBytes))
        }
      }
      // Bloom filter index file format:
      // numOfLong            4 Bytes, Int, record the total number of Longs in bit array
      // numOfHashFunction    4 Bytes, Int, record the total number of Hash Functions
      // elementCount         4 Bytes, Int, number of elements stored in the
      //                      related DataFile
      //
      // long 1               8 Bytes, Long, the first element in bit array
      // long 2               8 Bytes, Long, the second element in bit array
      // ...
      // long $numOfLong      8 Bytes, Long, the $numOfLong -th element in bit array
      //
      // dataEndOffset        8 Bytes, Long, data end offset
      // rootOffset           8 Bytes, Long, root Offset
      val bfBitArray = bfIndex.getBitMapLongArray
      var offset = 0L
      IndexUtils.writeInt(writer, bfBitArray.length) // bfBitArray length
      IndexUtils.writeInt(writer, bfIndex.getNumOfHashFunc) // numOfHashFunc
      IndexUtils.writeInt(writer, elemCnt)
      offset += 12
      bfBitArray.foreach(l => {
        IndexUtils.writeLong(writer, l)
        offset += 8
      })
      IndexUtils.writeLong(writer, offset) // dataEnd
      IndexUtils.writeLong(writer, offset) // rootOffset
      // writer.close()
      taskReturn :+ IndexBuildResult(filename, elemCnt, "", new Path(filename).getParent.toString)
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
