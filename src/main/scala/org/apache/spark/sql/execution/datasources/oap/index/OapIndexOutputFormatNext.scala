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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.Progressable
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

private[index] class OapIndexOutputFormatNext()
    extends FileOutputFormat[Void, InternalRow] {

  override def getRecordWriter(
      taskAttemptContext: TaskAttemptContext): RecordWriter[Void, InternalRow] = {

    val configuration = ContextUtil.getConfiguration(taskAttemptContext)

    val extension = "." + configuration.get(OapIndexFileFormatNext.INDEX_TIME) +
        "." + configuration.get(OapIndexFileFormatNext.INDEX_NAME) +
        ".index"

    val file = getDefaultWorkFile(taskAttemptContext, extension)

    val schema = StructType.fromString(configuration.get(OapIndexFileFormatNext.ROW_SCHEMA))

    new BTreeIndexRecordWriter(OapIndexFileWriter(configuration, schema, file), schema)
  }
}
