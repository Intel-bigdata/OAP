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

package org.apache.spark.sql.execution.datasources.oap.io.meta

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.parquet.column.statistics._

import org.apache.spark.sql.execution.datasources.oap.io.DataFileMeta
import org.apache.spark.sql.types._

abstract class OapDataFileMeta extends DataFileMeta

private[oap] class ColumnStatistics(val min: Array[Byte], val max: Array[Byte]) {
  def hasNonNullValue: Boolean = min != null && max != null

  def isEmpty: Boolean = !hasNonNullValue
}

private[oap] object ColumnStatistics {

  type ParquetStatistics = org.apache.parquet.column.statistics.Statistics[_ <: Comparable[_]]

  def getStatsBasedOnType(dataType: DataType): ParquetStatistics = {
    dataType match {
      case BooleanType => new BooleanStatistics()
      case IntegerType | ByteType | DateType | ShortType => new IntStatistics()
      case StringType | BinaryType => new BinaryStatistics()
      case FloatType => new FloatStatistics()
      case DoubleType => new DoubleStatistics()
      case LongType => new LongStatistics()
      case _ => sys.error(s"Not support data type: $dataType")
    }
  }

  def getStatsFromSchema(schema: StructType): Seq[ParquetStatistics] = {
    schema.map{ field => getStatsBasedOnType(field.dataType)}
  }

  def apply(stat: ParquetStatistics): ColumnStatistics = {
    if (!stat.hasNonNullValue) {
      new ColumnStatistics(null, null)
    } else {
      new ColumnStatistics(stat.getMinBytes, stat.getMaxBytes)
    }
  }

  def apply(in: DataInputStream): ColumnStatistics = {

    val minLength = in.readInt()
    val min = if (minLength != 0) {
      val bytes = new Array[Byte](minLength)
      in.readFully(bytes)
      bytes
    } else {
      null
    }

    val maxLength = in.readInt()
    val max = if (maxLength != 0) {
      val bytes = new Array[Byte](maxLength)
      in.readFully(bytes)
      bytes
    } else {
      null
    }

    new ColumnStatistics(min, max)
  }

  def unapply(statistics: ColumnStatistics): Option[Array[Byte]] = {
    val buf = new ByteArrayOutputStream()
    val out = new DataOutputStream(buf)

    if (statistics.hasNonNullValue) {
      out.writeInt(statistics.min.length)
      out.write(statistics.min)
      out.writeInt(statistics.max.length)
      out.write(statistics.max)
    } else {
      out.writeInt(0)
      out.writeInt(0)
    }

    Some(buf.toByteArray)
  }
}
