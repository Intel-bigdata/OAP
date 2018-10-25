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
package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.types.StructType

/**
 * `ReadOnlyNativeOrcFileFormat` only supports read native orc operation and not support write.
 * In oap we use it to create and refresh index because isSplitable method always returns false.
 */
class ReadOnlyNativeOrcFileFormat extends OrcFileFormat{
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    throw new UnsupportedOperationException(
      "ReadOnlyNativeOrcFileFormat doesn't support write operation")
}
