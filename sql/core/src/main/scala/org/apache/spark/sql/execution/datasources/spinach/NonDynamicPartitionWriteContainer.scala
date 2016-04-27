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

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{DefaultWriterContainer, WriteResult}
import org.apache.spark.sql.types.StructType

private[spinach] class NonDynamicPartitionWriteContainer(
  relation: SpinachRelation,
  @transient job: Job,
  isAppend: Boolean,
  schema: StructType) extends DefaultWriterContainer(relation, job, isAppend) {
  override def writeRows(
      taskContext: TaskContext,
      iterator: Iterator[InternalRow]): WriteResult = {
    // TODO if we need to wrap the row group
    executorSideSetup(taskContext)
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
    configuration.set("spark.sql.sources.output.path", outputPath)
    val writer = newOutputWriter(getWorkPath)

    writer.initConverter(dataSchema)

    var rowsWritten = 0
    var writerClosed = false

    val outputFileName = writer match {
      case s: SpinachOutputWriter => s.getFileName()
      case _ => ""
    }

    // If anything below fails, we should abort the task.
    try {
      while (iterator.hasNext) {
        val internalRow = iterator.next()
        writer.writeInternal(internalRow)
        rowsWritten += 1
      }
      commitTask()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }

    def commitTask(): Unit = {
      try {
        assert(writer != null, "OutputWriter instance should have been initialized")
        if (!writerClosed) {
          writer.close()
          writerClosed = true
        }
        super.commitTask()
      } catch {
        case cause: Throwable =>
          // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and
          // will cause `abortTask()` to be invoked.
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        if (!writerClosed) {
          writer.close()
          writerClosed = true
        }
      } finally {
        super.abortTask()
      }
    }

    WriteResult(outputFileName, rowsWritten)
  }

  override def commitJob(writeResults: Array[WriteResult]): Unit = {
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)
    val outputRoot = FileOutputFormat.getOutputPath(job)
    val path = new Path(outputRoot, SpinachFileFormat.SPINACH_META_FILE)
    val fileOut: FSDataOutputStream = outputRoot.getFileSystem(conf).create(path, false)

    val builder = DataSourceMeta.newBuilder()
    // The file fingerprint is not used at the moment.
    writeResults.foreach(r => builder.addFileMeta(FileMeta("", r.rowsWritten, r.fileName)))

    val spinachMeta = builder.withNewSchema(schema).build()
    DataSourceMeta.write(path, relation.sqlContext.sparkContext.hadoopConfiguration, spinachMeta)

    fileOut.close()
    super.commitJob(writeResults)
  }

  override def abortJob(): Unit = {
    // TODO do some clean up.
    super.abortJob()
  }
}
