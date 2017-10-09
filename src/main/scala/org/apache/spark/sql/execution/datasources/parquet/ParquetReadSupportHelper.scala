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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

object ParquetReadSupportHelper {

  val readSupport = new ParquetReadSupport

  def init(context: InitContext): ReadContext = readSupport.init(context)

  def prepareForRead(
                      conf: Configuration,
                      keyValueMetaData: JMap[String, String],
                      fileSchema: MessageType,
                      readContext: ReadContext): RecordMaterializer[UnsafeRow]
  = readSupport.prepareForRead(conf, keyValueMetaData, fileSchema, readContext)

  val SPARK_ROW_REQUESTED_SCHEMA = ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA

}
