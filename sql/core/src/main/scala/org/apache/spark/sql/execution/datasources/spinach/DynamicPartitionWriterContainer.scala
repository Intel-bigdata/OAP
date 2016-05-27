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

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitioningUtils, BaseWriterContainer}
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.sources.{HadoopFsRelation, OutputWriter}
import org.apache.spark.sql.types.{StructType, StringType}

private[spinach] case class SpinachPartitionWriteResult(
    partition: String,
    fileName: String,
    rowsWritten: Int)

private[spinach] case class SpinachDynamicPartitionWriteResult(
    results: Seq[SpinachPartitionWriteResult])

/**
 * Adapted from [[org.apache.spark.sql.execution.datasources.DynamicPartitionWriterContainer]]
 * A writer that dynamically opens files based on the given partition columns.  Internally this is
 * done by maintaining a HashMap of open files until `maxFiles` is reached.  If this occurs, the
 * writer externally sorts the remaining rows and then writes out them out one file at a time.
 */
private[spinach] class DynamicPartitionWriterContainer(
    relation: HadoopFsRelation,
    @transient job: Job,
    partitionColumns: Seq[Attribute],
    dataColumns: Seq[Attribute],
    inputSchema: Seq[Attribute],
    defaultPartitionName: String,
    maxOpenFiles: Int,
    isAppend: Boolean,
    schema: StructType)
  extends BaseWriterContainer(relation, job, isAppend) {

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): WriteResult = {
    val outputWriters = new java.util.HashMap[InternalRow, OutputWriter]
    // The out file name is a relative path including the partition key like col1=val
    val outputFileNames = new java.util.HashMap[InternalRow, String]
    val rowsWrittens = new java.util.HashMap[InternalRow, Int]
    executorSideSetup(taskContext)

    var outputWritersCleared = false

    // Returns the partition key given an input row
    val getPartitionKey = UnsafeProjection.create(partitionColumns, inputSchema)
    // Returns the data columns to be written given an input row
    val getOutputRow = UnsafeProjection.create(dataColumns, inputSchema)

    // Expressions that given a partition key build a string like: col1=val/col2=val/...
    val partitionStringExpression = partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(
          PartitioningUtils.escapePathName _, StringType, Seq(Cast(c, StringType)), Seq(StringType))
      val str = If(IsNull(c), Literal(defaultPartitionName), escaped)
      val partitionName = Literal(c.name + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR) :: partitionName
    }

    // Returns the partition path given a partition key.
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionColumns)

    // If anything below fails, we should abort the task.
    try {
      // This will be filled in if we have to fall back on sorting.
      var sorter: UnsafeKVExternalSorter = null
      while (iterator.hasNext && sorter == null) {
        val inputRow = iterator.next()
        val currentKey = getPartitionKey(inputRow)
        val copiedKey = currentKey.copy()
        var currentWriter = outputWriters.get(currentKey)

        if (currentWriter == null && outputWriters.size >= maxOpenFiles) {
          logInfo(s"Maximum partitions reached, falling back on sorting.")
          sorter = new UnsafeKVExternalSorter(
            StructType.fromAttributes(partitionColumns),
            StructType.fromAttributes(dataColumns),
            SparkEnv.get.blockManager,
            TaskContext.get().taskMemoryManager().pageSizeBytes)
          sorter.insertKV(currentKey, getOutputRow(inputRow))
        } else {
          if (currentWriter == null) {
            currentWriter = newOutputWriter(currentKey)
            outputWriters.put(copiedKey, currentWriter)
            rowsWrittens.put(copiedKey, 0)
            outputFileNames.put(copiedKey, getOutputFileName(currentWriter))
          }
          currentWriter.writeInternal(getOutputRow(inputRow))
          rowsWrittens.put(copiedKey, rowsWrittens.get(copiedKey) + 1)
        }
      }

      // If the sorter is not null that means that we reached the maxFiles above and need to finish
      // using external sort.
      if (sorter != null) {
        while (iterator.hasNext) {
          val currentRow = iterator.next()
          sorter.insertKV(getPartitionKey(currentRow), getOutputRow(currentRow))
        }

        logInfo(s"Sorting complete. Writing out partition files one at a time.")

        val sortedIterator = sorter.sortedIterator()
        var currentKey: InternalRow = null
        var currentWriter: OutputWriter = null
        try {
          while (sortedIterator.next()) {
            if (currentKey != sortedIterator.getKey) {
              if (currentWriter != null) {
                currentWriter.close()
              }
              currentKey = sortedIterator.getKey.copy()
              logDebug(s"Writing partition: $currentKey")

              // Either use an existing file from before, or open a new one.
              currentWriter = outputWriters.remove(currentKey)
              if (currentWriter == null) {
                currentWriter = newOutputWriter(currentKey)
                rowsWrittens.put(currentKey, 0)
                outputFileNames.put(currentKey, getOutputFileName(currentWriter))
              }
            }

            currentWriter.writeInternal(sortedIterator.getValue)
            rowsWrittens.put(currentKey, rowsWrittens.get(currentKey) + 1)
          }
        } finally {
          if (currentWriter != null) { currentWriter.close() }
        }
      }

      commitTask()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }

     def getOutputFileName(writer: OutputWriter): String = {
       writer match {
          case s: SpinachOutputWriter => s.getFileName
          case _ => throw new SparkException("Incorrect Spinach output writer.")
        }
     }

    /** Open and returns a new OutputWriter given a partition key. */
    def newOutputWriter(key: InternalRow): OutputWriter = {
      val partitionPath = getPartitionString(key).getString(0)
      val path = new Path(getWorkPath, partitionPath)
      val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
      configuration.set(
        "spark.sql.sources.output.path", new Path(outputPath, partitionPath).toString)
      val newWriter = super.newOutputWriter(path.toString)
      newWriter.initConverter(dataSchema)
      newWriter
    }

    def clearOutputWriters(): Unit = {
      if (!outputWritersCleared) {
        outputWriters.asScala.values.foreach(_.close())
        outputWriters.clear()
        outputWritersCleared = true
      }
    }

    def commitTask(): Unit = {
      try {
        clearOutputWriters()
        super.commitTask()
      } catch {
        case cause: Throwable =>
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        clearOutputWriters()
      } finally {
        super.abortTask()
      }
    }

    SpinachDynamicPartitionWriteResult(outputFileNames.keySet().toArray.map { key =>
      SpinachPartitionWriteResult(
        getPartitionString(key.asInstanceOf[InternalRow]).getString(0),
        outputFileNames.get(key),
        rowsWrittens.get(key))
    })
  }

  override def commitJob(writeResults: Array[WriteResult]): Unit = {
    val outputRoot = FileOutputFormat.getOutputPath(job)

    val partitions = writeResults.flatMap { case d: SpinachDynamicPartitionWriteResult =>
      d.results.map(_.partition)
    }.distinct

    partitions.foreach { p =>
      val builder = DataSourceMeta.newBuilder()
      writeResults.foreach {
        case d: SpinachDynamicPartitionWriteResult =>
          for (r <- d.results if (r.partition == p)) {
            // The file fingerprint is not used at the moment.
            builder.addFileMeta(FileMeta("", r.rowsWritten, r.fileName))
          }
        case _ => throw new SparkException("Unexpected Spinach write result.")
      }
      val spinachMeta = builder.withNewSchema(schema).build()
      val path = new Path(new Path(outputRoot, p), SpinachFileFormat.SPINACH_META_FILE)
      DataSourceMeta.write(path, relation.sqlContext.sparkContext.hadoopConfiguration, spinachMeta)
    }

    super.commitJob(writeResults)
  }
}
