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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport, SpinachReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.SpinachReadSupport.SpinachReadContext
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupportHelper, UnsafeRowParquetRecordMaterializer}
import org.apache.spark.sql.types._


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
class SpinachReadSupportImpl extends SpinachReadSupport[UnsafeRow] with Logging {

  private var catalystRequestedSchema: StructType = _
  private var catalystReadFromFileSchema: StructType = _

  /**
   * Called on executor side before [[prepareForRead()]] and instantiating actual Parquet record
   * readers.  Responsible for figuring out Parquet requested schema used for column pruning.
   */
  override def init(context: InitContext): SpinachReadContext = {
    catalystRequestedSchema = {
      val conf = context.getConfiguration
      val schemaString = conf.get(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA)
      assert(schemaString != null, "Parquet requested schema not set.")
      StructType.fromString(schemaString)
    }

    val spinachRequestedSchema =
      ParquetReadSupportHelper.clipParquetSchema(context.getFileSchema, catalystRequestedSchema)

    catalystReadFromFileSchema = {
      val conf = context.getConfiguration
      val readFromFileSchemaString =
        conf.get(SpinachReadSupportImpl.SPARK_ROW_READ_FROM_FILE_SCHEMA)
      if (readFromFileSchemaString == null) {
        StructType.fromString(conf.get(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA))
      } else {
        StructType.fromString(readFromFileSchemaString)
      }
    }

    val spinachReadFromFileSchema =
      ParquetReadSupportHelper.clipParquetSchema(context.getFileSchema, catalystReadFromFileSchema)

    val readContext = new ReadContext(spinachRequestedSchema, Map.empty[String, String].asJava)
    new SpinachReadContext(spinachReadFromFileSchema, readContext)
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
                               readContext: SpinachReadContext): RecordMaterializer[UnsafeRow] = {
    log.debug(s"Preparing for read Parquet file with message type: $fileSchema")
    val parquetRequestedSchema = readContext.getRequestedSchema

    logInfo {
      s"""Going to read the following fields from the Parquet file:
          |
         |Parquet form:
          |$parquetRequestedSchema
          |Catalyst form:
          |$catalystRequestedSchema
       """.stripMargin
    }

    new UnsafeRowParquetRecordMaterializer(
      parquetRequestedSchema,
      ParquetReadSupportHelper.expandUDT(catalystRequestedSchema))
  }
}

object SpinachReadSupportImpl {

  val SPARK_ROW_READ_FROM_FILE_SCHEMA = "org.apache.spark.sql.parquet.row.read_from_file_schema"
}
