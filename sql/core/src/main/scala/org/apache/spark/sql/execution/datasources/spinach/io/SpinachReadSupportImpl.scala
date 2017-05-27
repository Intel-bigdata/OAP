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

package org.apache.spark.sql.execution.datasources.spinach.io

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper


/**
 * A Parquet [[ReadSupport]] implementation for reading Parquet records as Catalyst
 * [[InternalRow]]s.
 *
 * The API interface of [[ReadSupport]] is a little bit over complicated because of
 * historical reasons.  In older versions of parquet-mr (say 1.6.0rc3 and prior),
 * [[ReadSupport]] need to be instantiated and initialized twice on both driver side
 * and executor side.  The [[init()]] method is for driver side initialization,
 * while [[prepareForRead()]] is for executor side.  However, starting from
 * parquet-mr 1.6.0, it's no longer the case, and [[ReadSupport]] is only
 * instantiated and initialized on executor side.
 * So, theoretically, now it's totally fine to combine these two methods
 * into a single initialization method.
 * The only reason (I could think of) to still have them here
 * is for parquet-mr API backwards-compatibility.
 *
 * Due to this reason, we no longer rely on [[ReadContext]]
 * to pass requested schema from [[init()]]
 * to [[prepareForRead()]], but use a private `var` for simplicity.
 */
class SpinachReadSupportImpl extends ReadSupport[InternalRow] with Logging {


  /**
   * Called on executor side before [[prepareForRead()]] and instantiating actual Parquet record
   * readers.  Responsible for figuring out Parquet requested schema used for column pruning.
   */
  override def init(context: InitContext): ReadContext = {
    ParquetReadSupportHelper.init(context)
  }

  /**
   * Called on executor side after [[init()]], before instantiating actual Parquet record readers.
   * Responsible for instantiating [[RecordMaterializer]], which is used for converting Parquet
   * records to Catalyst [[InternalRow]]s.
   */
  override def prepareForRead(
                               conf: Configuration,
                               keyValueMetaData: JMap[String, String],
                               fileSchema: MessageType,
                               readContext: ReadContext): RecordMaterializer[InternalRow] = {
    ParquetReadSupportHelper.prepareForRead(conf, keyValueMetaData, fileSchema, readContext)
  }
}

