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

package org.apache.spark.sql.execution.datasources.oap

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriter, WriteResult}
import org.apache.spark.sql.execution.datasources.oap.io.OapDataWriter
import org.apache.spark.sql.types.StructType


private[oap] class OapOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext) extends OutputWriter {
  private var rowCount = 0
  private var partitionString: String = ""

  override def setPartitionString(ps: String): Unit = {
    partitionString = ps
  }

  private val writer: OapDataWriter = {
    val isCompressed = FileOutputFormat.getCompressOutput(context)
    val conf = context.getConfiguration
    val file: Path = new Path(path)
    val fs = file.getFileSystem(conf)
    val fileOut = fs.create(file, false)

    new OapDataWriter(isCompressed, fileOut, dataSchema, conf)
  }

  override def write(row: InternalRow): Unit = {
    rowCount += 1
    writer.write(row)
  }

  override def close(): Unit = {
    writer.close()
  }

  override def writeStatus(): WriteResult = {
    OapWriteResult(dataFileName, rowCount, partitionString)
  }

  def dataFileName: String = new Path(path).getName
}
