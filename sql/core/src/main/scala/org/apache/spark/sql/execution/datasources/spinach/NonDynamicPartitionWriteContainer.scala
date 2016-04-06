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
import org.apache.spark.TaskContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.DefaultWriterContainer
import org.apache.spark.sql.types.StructType

private[spinach] class NonDynamicPartitionWriteContainer(
  relation: SpinachRelation,
  @transient job: Job,
  isAppend: Boolean,
  schema: StructType) extends DefaultWriterContainer(relation, job, isAppend) {
  override def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    // TODO if we need to wrap the row group
    super.writeRows(taskContext, iterator)
  }

  override def commitJob(): Unit = {
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)
    val outputRoot = FileOutputFormat.getOutputPath(job)
    val fileOut: FSDataOutputStream =
      outputRoot.getFileSystem(conf).create(
        new Path(outputRoot, SpinachFileFormat.SPINACH_META_FILE), false)

    writeMeta(fileOut) // write the meta data

    fileOut.close()
    // TODO write the meta data
    super.commitJob()
  }

  override def abortJob(): Unit = {
    // TODO do some clean up.
    super.abortJob()
  }

  private def writeMeta(metaFSOut: FSDataOutputStream): Unit = {
    metaFSOut.writeUTF(schema.json)
  }
}
