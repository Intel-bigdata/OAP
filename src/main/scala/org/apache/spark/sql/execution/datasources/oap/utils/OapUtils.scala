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

package org.apache.spark.sql.execution.datasources.oap.utils

import java.lang.{Double => JDouble, Float => JFloat}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.io.api.Binary

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.execution.datasources.{FileCatalog, Partition, PartitionSpec}
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, Key, OapFileFormat}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


/**
 * Utils for Oap
 */
object OapUtils extends Logging {
  def getMeta(hadoopConf: Configuration, parent: Path): Option[DataSourceMeta] = {
    val file = new Path(parent, OapFileFormat.OAP_META_FILE)
    if (file.getFileSystem(hadoopConf).exists(file)) {
      Some(DataSourceMeta.initialize(file, hadoopConf))
    } else {
      None
    }
  }

  def getPartitions(fileCatalog: FileCatalog,
                    partitionSpec: Option[TablePartitionSpec] = None): Seq[Partition] = {
    val filters = if (partitionSpec.nonEmpty) {
      val PartitionSpec(partitionColumns, _) = fileCatalog.partitionSpec()
      val partitionColumnsInfo: Map[String, DataType] =
        partitionColumns.map {
          field => (field.name, field.dataType)
        }.toMap
      // partition column spec check
      if (!partitionSpec.get.keys.forall(
        partitionColumnsInfo.keys.toSeq.contains(_))) {
        throw new AnalysisException(
          s"Partition spec is invalid. The spec (${partitionSpec.get.keys.mkString(", ")})" +
            s" must match the partition spec (${partitionColumnsInfo.keys.mkString(", ")})")
      }
      partitionSpec.get.map { case (key, value) =>
        val v = partitionColumnsInfo.get(key).get match {
          case StringType => value
          case IntegerType => value.toInt
          case LongType => value.toLong
          case BooleanType => value.toBoolean
          case _: DataType =>
            throw new AnalysisException(
              s"Only handle partition key type in common use, check the partition key type:" +
                s" ${partitionColumnsInfo.get(key).get.toString}")
        }
        EqualTo(AttributeReference(key, partitionColumnsInfo.get(key).get)(), Literal(v))
      }.toSeq
    } else Nil
    fileCatalog.listFiles(filters)
  }

  def keyFromBytes(bytes: Array[Byte], dataType: DataType): Option[Key] = {
    val value: Option[Any] = dataType match {
      case BooleanType => Some(BytesUtils.bytesToBool(bytes))
      case IntegerType => Some(BytesUtils.bytesToInt(bytes))
      case LongType => Some(BytesUtils.bytesToLong(bytes))
      case DoubleType => Some(JDouble.longBitsToDouble(BytesUtils.bytesToLong(bytes)))
      case FloatType => Some(JFloat.intBitsToFloat(BytesUtils.bytesToInt(bytes)))
      case StringType => Some(UTF8String.fromBytes(bytes))
      case BinaryType => Some(Binary.fromReusedByteArray(bytes))
      case _ => None
    }
    value.map(v => InternalRow(CatalystTypeConverters.convertToCatalyst(v)))
  }
  def keyFromAny(value: Any): Key = InternalRow(CatalystTypeConverters.convertToCatalyst(value))
}

