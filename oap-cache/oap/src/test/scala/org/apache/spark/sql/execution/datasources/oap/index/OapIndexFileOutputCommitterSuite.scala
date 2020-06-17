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

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class OapIndexFileOutputCommitterSuite extends SparkFunSuite {
  // A task attempt id for testing.
  private val attempt = "attempt_200707121733_0001_m_000000_0"
  private val task = "task_200707121733_0001_m_000000"
  private val partFile = "part-m-00000"
  private val taskID = TaskAttemptID.forName(attempt)

  test("test job and task output in committer algorithm v2") {
    val outDir = new Path(Utils.createTempDir().getCanonicalPath)
    val job = Job.getInstance
    FileOutputFormat.setOutputPath(job, outDir)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt)
    val jContext = new JobContextImpl(conf, taskID.getJobID())
    val tContext = new TaskAttemptContextImpl(conf, taskID)
    val pendingDirName = "_temporary_" + jContext.getJobID.toString
    val algoVersion = 2
    val committer = new OapIndexFileOutputCommitter(outDir, tContext, pendingDirName, algoVersion)
    // test job attempt path
    val jobAttemptPath = committer.getJobAttemptPath(jContext, outDir)
    val expectedJobAttemptPath = new Path(new Path(outDir, pendingDirName), "0")
    assert(jobAttemptPath == expectedJobAttemptPath)

    // test task attempt path
    val expectedTaskWorkPath = new Path(new Path(expectedJobAttemptPath, pendingDirName), attempt)
    assert(committer.getWorkPath == expectedTaskWorkPath)

    // setup
    committer.setupJob(jContext)
    committer.setupTask(tContext)

    // test commit task path
    val fs = FileSystem.get(job.getConfiguration)
    val testFile = new Path(expectedTaskWorkPath, partFile)
    fs.mkdirs(expectedTaskWorkPath)
    fs.create(testFile).write(0)
    committer.commitTask(tContext)
    checkOutput(fs, outDir)

    // test commit job path
    committer.commitJob(jContext)
    checkOutput(fs, outDir)
  }

  test("test job and task output in committer algorithm v1") {
    val outDir = new Path(Utils.createTempDir().getCanonicalPath)
    val job = Job.getInstance
    FileOutputFormat.setOutputPath(job, outDir)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt)
    val jContext = new JobContextImpl(conf, taskID.getJobID())
    val tContext = new TaskAttemptContextImpl(conf, taskID)
    val pendingDirName = "_temporary_" + jContext.getJobID.toString
    val algoVersion = 1
    val committer = new OapIndexFileOutputCommitter(outDir, tContext, pendingDirName, algoVersion)
    // test job attempt path
    val jobAttemptPath = committer.getJobAttemptPath(jContext, outDir)
    val expectedJobAttemptPath = new Path(new Path(outDir, pendingDirName), "0")
    assert(jobAttemptPath == expectedJobAttemptPath)

    // test task attempt path
    val expectedTaskWorkPath = new Path(new Path(expectedJobAttemptPath, pendingDirName), attempt)
    assert(committer.getWorkPath == expectedTaskWorkPath)

    // setup
    committer.setupJob(jContext)
    committer.setupTask(tContext)

    // test commit task path
    val fs = FileSystem.get(job.getConfiguration)
    val testFile = new Path(expectedTaskWorkPath, partFile)
    fs.mkdirs(expectedTaskWorkPath)
    fs.create(testFile).write(0)
    committer.commitTask(tContext)

    val expectedCommittedTaskPath = new Path(expectedJobAttemptPath, task)
    checkOutput(fs, expectedCommittedTaskPath)

    // test commit job path
    committer.commitJob(jContext)
    checkOutput(fs, outDir)
  }

  private def checkOutput(fs: FileSystem, expectedMapDir: Path): Unit = {
    assert(fs.getFileStatus(expectedMapDir).isDirectory)
    val files = fs.listStatus(expectedMapDir)
    var fileCount = 0
    var dataFileFound = false
    for (f <- files) {
      if (f.isFile) {
        fileCount += 1
        if (f.getPath.getName == partFile) dataFileFound = true
      }
    }
    assert(fileCount > 0)
    assert(dataFileFound)
  }
}
