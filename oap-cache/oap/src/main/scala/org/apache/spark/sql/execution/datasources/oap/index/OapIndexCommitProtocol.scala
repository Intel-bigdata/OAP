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
import org.apache.hadoop.mapreduce.OutputCommitter
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.sql.internal.oap.OapConf

/**
 * A variant of [[HadoopMapReduceCommitProtocol]] that uses OapIndexFileOutputCommitter as
 * the actual output committer.
 */
class OapIndexCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite)
    with Serializable with Logging {

  /** OutputCommitter from Hadoop is not serializable so marking it transient. */
  @transient private var committer: OapIndexFileOutputCommitter = _

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val algorithmVersion =
      SparkEnv.get.conf.get(OapConf.OAP_INDEXFILEOUTPUTCOMMITTER_ALGORITHM_VERSION)
    val tempDirName = s"_temporary_$jobId"
    committer = new OapIndexFileOutputCommitter(
      new Path(path), context, tempDirName, algorithmVersion)
    logInfo(s"Using output committer class ${committer.getClass.getCanonicalName}")
    committer
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val filename = getFilename(taskContext, ext)
    val stagingDir = new Path(Option(committer.getWorkPath).map(_.toString).getOrElse(path))
    dir.map { d =>
      new Path(new Path(stagingDir, d), filename).toString
    }.getOrElse {
      new Path(stagingDir, filename).toString
    }
  }

  override protected def getFilename(taskContext: TaskAttemptContext, ext: String): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }
}

