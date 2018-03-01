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

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.types.StructType

private[index] class OapIndexFileFormat
  extends FileFormat
  with Logging
  with Serializable {

  override def inferSchema: Option[StructType] = None

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val configuration = ContextUtil.getConfiguration(job)

    configuration.set(OapIndexFileFormat.ROW_SCHEMA, dataSchema.json)
    configuration.set(OapIndexFileFormat.INDEX_TYPE, options("indexType"))
    configuration.set(OapIndexFileFormat.INDEX_NAME, options("indexName"))
    configuration.set(OapIndexFileFormat.INDEX_TIME, options("indexTime"))
    configuration.set(OapIndexFileFormat.IS_APPEND, options("isAppend"))

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String =
        OapFileFormat.OAP_INDEX_EXTENSION

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext) =
        new OapIndexOutputWriter(path, context)
    }
  }
}

private[index] object OapIndexFileFormat {
  val ROW_SCHEMA: String = "org.apache.spark.sql.oap.row.attributes"
  val INDEX_TYPE: String = "org.apache.spark.sql.oap.index.type"
  val INDEX_NAME: String = "org.apache.spark.sql.oap.index.name"
  val INDEX_TIME: String = "org.apache.spark.sql.oap.index.time"
  val IS_APPEND: String = "org.apache.spark.sql.oap.index.append"
}

case class IndexBuildResult(dataFile: String, rowCount: Long, fingerprint: String, parent: String)
