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

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormatWriter, WriteResult}
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile


private[index] abstract class IndexWriter extends FileFormatWriter {

  private var shouldCloseWriter: Boolean = false

  class IndexWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol) extends ExecuteWriteTask {

    var outputWriter: IndexOutputWriter = {
      val tmpFilePath = committer.newTaskTempFile(
        taskAttemptContext,
        None,
        description.outputWriterFactory.getFileExtension(taskAttemptContext))

      val outputWriter = description.outputWriterFactory.newInstance(
        path = tmpFilePath,
        dataSchema = description.nonPartitionColumns.toStructType,
        context = taskAttemptContext)
      outputWriter.initConverter(dataSchema = description.nonPartitionColumns.toStructType)
      outputWriter.asInstanceOf[IndexOutputWriter]
    }

    override def execute(iter: Iterator[InternalRow]): (Set[String], Seq[WriteResult]) = {
      var result = writeIndexFromRows(description, outputWriter, iter)
      shouldCloseWriter = result != Nil
      (Set.empty, result)
    }

    override def releaseResources(): WriteResult = {
      var res: WriteResult = Nil
      if (shouldCloseWriter && outputWriter != null) {
        res = outputWriter.close()
        outputWriter = null
      }
      res
    }
  }

  override def getWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol): Option[ExecuteWriteTask] = {
    Some(new IndexWriteTask(description, taskAttemptContext, committer))
  }

  def writeIndexFromRows(
      description: WriteJobDescription,
      writer: IndexOutputWriter,
      iterator: Iterator[InternalRow]): Seq[IndexBuildResult]

  protected def writeHead(writer: IndexOutputWriter, version: Int): Int = {
    writer.write("OAPIDX".getBytes("UTF-8"))
    assert(version <= 65535)
    val data = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(data)
    IndexFile.indexFileHeaderLength
  }
}

private[index] object IndexWriter {
  val INPUT_FILE_NAME = "spark.sql.oap.inputFileName"
  val INDEX_NAME = "spark.sql.oap.indexName"
  val INDEX_TIME = "spark.sql.oap.indexTime"
}

case class IndexBuildResult(dataFile: String, rowCount: Long, fingerprint: String, parent: String)
