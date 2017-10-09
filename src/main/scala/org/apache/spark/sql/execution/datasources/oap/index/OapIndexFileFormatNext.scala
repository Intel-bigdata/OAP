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

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

private[index] class OapIndexFileFormatNext extends FileFormat {

  override def inferSchema: Option[StructType] = None

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val configuration = ContextUtil.getConfiguration(job)

    configuration.set(OapIndexFileFormatNext.ROW_SCHEMA, dataSchema.json)
    configuration.set(OapIndexFileFormatNext.INDEX_NAME, options("indexName"))
    configuration.set(OapIndexFileFormatNext.INDEX_TIME, options("indexTime"))

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OapIndexOutputWriter(path, bucketId, context)
      }
    }
  }
}

private[index] object OapIndexFileFormatNext {
  val ROW_SCHEMA: String = "org.apache.spark.sql.oap.row.attributes"
  val INDEX_NAME: String = "org.apache.spark.sql.oap.index.name"
  val INDEX_TIME: String = "org.apache.spark.sql.oap.index.time"
}
