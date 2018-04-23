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

package org.apache.spark.sql.execution.datasources.oap;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oap can use OapIndexFileOutputCommitter to optimize Commit Job phase of Index DDL operation.
 */
public class OapIndexFileOutputCommitter extends FileOutputCommitter {

    private Logger logger = LoggerFactory.getLogger(OapIndexFileOutputCommitter.class);

    private Path outputPath = null;

    public OapIndexFileOutputCommitter(
        Path outputPath,
        TaskAttemptContext context) throws IOException {
      super(outputPath, context);
      if (outputPath != null) {
        FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
        this.outputPath = fs.makeQualified(outputPath);
      }
    }

    public OapIndexFileOutputCommitter(
        Path outputPath,
        JobContext context) throws IOException {
      super(outputPath, context);
      if (outputPath != null) {
        FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
        this.outputPath = fs.makeQualified(outputPath);
      }
    }

    @Override
    public void commitTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
      super.commitTask(context, taskAttemptPath);
      // if FileOutputCommitter commit task no exception, try do Executor Side Commit Job.
      tryDoExecutorCommitJob(context);
    }

    private void tryDoExecutorCommitJob(TaskAttemptContext context) throws IOException {
      try {
        Path committedTaskPath = getCommittedTaskPath(context);
        FileSystem fs = committedTaskPath.getFileSystem(context.getConfiguration());
        FileStatus from = fs.getFileStatus(committedTaskPath);
        if (moveIndexFile(context, from)) {
          fs.delete(committedTaskPath, true);
        }
      } catch (FileNotFoundException e) {
        logger.error("getCommittedTaskPath not exist", e);
      }
    }

    private Path getOutputPath() {
      return this.outputPath;
    }

    private boolean hasOutputPath() {
      return this.outputPath != null;
    }

    private boolean moveIndexFile(TaskAttemptContext context, FileStatus from) throws IOException {
      if (hasOutputPath()) {
        Path finalOutput = getOutputPath();
        FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());
        mergePaths(fs, from, finalOutput);
        return true;
      } else {
        logger.warn("Output Path is null in moveTaskResultToTarget()");
        return false;
      }
    }

    /**
     * do mv -r 'from.getPath.getName' 'to.getName'
     * @param fs the File System to use
     * @param from the path data is coming from.
     * @param to the path data is going to.
     * @throws IOException on any error
     */
    private void mergePaths(
        FileSystem fs,
        final FileStatus from,
        final Path to) throws IOException {
      logger.debug("Merging Index file from " + from + " to " + to);
      if (from.isFile()) {
        if (fs.exists(to)) {
          // for refresh cmd.
          if (!fs.delete(to, true)) {
            throw new IOException("Failed to delete " + to);
          }
        }
        if (!fs.rename(from.getPath(), to)) {
          throw new IOException("Failed to rename Index File " + from + " to " + to);
        }
      } else if(from.isDirectory()) {
        if (fs.exists(to)) {
          FileStatus toStat = fs.getFileStatus(to);
          if (!toStat.isDirectory()) {
            throw new IOException("Index ddl Target " + to + " must be a directory.");
          } else {
            for (FileStatus subFrom : fs.listStatus(from.getPath())) {
              Path subTo = new Path(to, subFrom.getPath().getName());
              mergePaths(fs, subFrom, subTo);
            }
          }
        } else {
          throw new IOException("IndexBuild commit target directory " + to + " should exist");
        }
      }
    }
}
