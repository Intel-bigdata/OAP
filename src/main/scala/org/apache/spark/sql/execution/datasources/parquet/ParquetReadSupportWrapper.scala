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
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * Wrap ParquetReadSupport, it can be used outside the parquet package.
 * ParquetReadSupport extends ReadSupport[UnsafeRow] with Logging,
 * so ParquetReadSupportWrapper use the same hierarchy of inheritance.
 */
class ParquetReadSupportWrapper extends ReadSupport[UnsafeRow] with Logging {

  // Proxy ParquetReadSupport instance to use.
  private val readSupport = new ParquetReadSupport

  /**
   * Proxy ParquetReadSupport#init method.
   */
  override def init(context: InitContext): ReadContext = {
    readSupport.init(context)
  }

  /**
   * Proxy ParquetReadSupport#prepareForRead method.
   */
  override def prepareForRead(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[UnsafeRow] = {
    readSupport.prepareForRead(conf, keyValueMetaData, fileSchema, readContext)
  }
}
object ParquetReadSupportWrapper {
  // Proxy ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA value.
  val SPARK_ROW_REQUESTED_SCHEMA: String = ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA
}

