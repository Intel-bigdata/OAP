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

import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{BaseWriterContainer, WriteResult}
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile

private[index] abstract class IndexWriter (
    relation: WriteIndexRelation,
    job: Job,
    isAppend: Boolean) extends BaseWriterContainer(relation.toWriteRelation, job, isAppend) {
  // TODO figure out right way to deal with bucket
  protected def newIndexOutputWriter(bucketId: Option[Int] = None): IndexOutputWriter = {
    try {
      outputWriterFactory.asInstanceOf[IndexOutputWriterFactory].newInstance(
        bucketId, dataSchema, taskAttemptContext)
    } catch {
      case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
        if (outputCommitter.getClass.getName.contains("Direct")) {
          // SPARK-11382: DirectParquetOutputCommitter is not idempotent, meaning on retry
          // attempts, the task will fail because the output file is created from a prior attempt.
          // This often means the most visible error to the user is misleading. Augment the error
          // to tell the user to look for the actual error.
          throw new SparkException("The output file already exists but this could be due to a " +
            "failure from an earlier attempt. Look through the earlier logs or stage page for " +
            "the first error.\n  File exists error: " + e, e)
        } else {
          throw e
        }
    }
  }

  def writeIndexFromRows(
      taskContext: TaskContext, iterator: Iterator[InternalRow]): Seq[IndexBuildResult]
  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Seq[WriteResult] =
    writeIndexFromRows(taskContext, iterator)

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
