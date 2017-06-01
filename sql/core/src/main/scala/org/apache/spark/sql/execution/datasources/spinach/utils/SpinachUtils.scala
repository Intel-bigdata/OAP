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

package org.apache.spark.sql.execution.datasources.spinach.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.execution.datasources.{FileCatalog, Partition, PartitionSpec}
import org.apache.spark.sql.execution.datasources.spinach.{DataSourceMeta, SpinachFileFormat}
import org.apache.spark.sql.types._


/**
 * Utils for Spinach
 */
object SpinachUtils {
  def getMeta(hadoopConf: Configuration, parent: Path): Option[DataSourceMeta] = {
    val file = new Path(parent, SpinachFileFormat.SPINACH_META_FILE)
    if (file.getFileSystem(hadoopConf).exists(file)) {
      Some(DataSourceMeta.initialize(file, hadoopConf))
    } else {
      None
    }
  }

  def getPartitions(fileCatalog: FileCatalog,
                    partitionSpec: Option[TablePartitionSpec]): Seq[Partition] = {
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
}

