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
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class OapIndexCommitProtocolSuite extends SharedOapContext {
  test("newTaskTempFile") {
    val attempt = "attempt_200707121733_0001_m_000000_0"
    val taskID = TaskAttemptID.forName(attempt)
    val jobID = taskID.getJobID.toString
    val outDir = Utils.createTempDir().getAbsolutePath
    val job = Job.getInstance()
    FileOutputFormat.setOutputPath(job, new Path(outDir))
    val conf = job.getConfiguration()
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt)
    val jobContext = new JobContextImpl(conf, taskID.getJobID())
    val taskContext = new TaskAttemptContextImpl(conf, taskID)
    val commitProtocol = new OapIndexCommitProtocol(jobID, outDir)
    // test task temp path
    val pendingDirName = "_temporary_" + jobID
    commitProtocol.setupJob(jobContext)
    commitProtocol.setupTask(taskContext)
    val tempFile = new Path(commitProtocol.newTaskTempFile(taskContext, None, "test"))
    val expectedJobAttemptPath = new Path(new Path(outDir, pendingDirName), "0")
    val expectedTaskWorkPath = new Path(new Path(expectedJobAttemptPath, pendingDirName), attempt)
    assert(tempFile.getParent == expectedTaskWorkPath)
  }
}
