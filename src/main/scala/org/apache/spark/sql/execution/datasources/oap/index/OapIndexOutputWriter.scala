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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.spark.rdd.InputFileNameHolder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter

private[index] class OapIndexOutputWriter(
    path: String,
    context: TaskAttemptContext
) extends OutputWriter {

  private val outputFormat = new OapIndexOutputFormat() {
    override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {

      val outputPath = FileOutputFormat.getOutputPath(context)
      val inputFile = new Path(inputFileName)
      val partitionDir = inputFile.getParent.toString.replace(outputPath.toString, "")

      val dataName = inputFile.getName
      val pos = dataName.lastIndexOf(".")
      val indexFileName = if (pos > 0) {
        dataName.substring(0, pos)
      } else {
        dataName
      }

      // Workaround: FileFormatWriter passes a temp file name to us. But index file name is not
      // a random name. So we only use the upper directory.
      new Path(new Path(path).getParent + partitionDir, "." + indexFileName + extension)
    }
  }

  private var recordWriter: RecordWriter[Void, InternalRow] = _

  private var inputFileName: String = _

  private var results: Seq[IndexBuildResult] = Nil

  private var rowCount: Long = 0

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override def writeInternal(row: InternalRow): Unit = {
    checkStartOfNewFile()
    recordWriter.write(null, row)
    rowCount += 1
  }

  override def close(): Seq[IndexBuildResult] = {
    closeWriter()
    results
  }

  private def initWriter(): Unit = {
    inputFileName = InputFileNameHolder.getInputFileName().toString
    recordWriter = outputFormat.getRecordWriter(context)
    rowCount = 0
  }

  private def closeWriter(): Unit = {
    if (recordWriter != null) {
      recordWriter.close(context)
      recordWriter = null
      results = results :+
          IndexBuildResult(inputFileName, rowCount, "", new Path(inputFileName).getParent.toString)
    }
  }

  private def checkStartOfNewFile(): Unit = {
    if (inputFileName != InputFileNameHolder.getInputFileName().toString) {
      closeWriter()
      initWriter()
    }
  }
}

