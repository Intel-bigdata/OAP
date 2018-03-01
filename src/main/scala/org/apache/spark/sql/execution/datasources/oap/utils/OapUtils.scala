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

import scala.util.Failure

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.io.api.Binary

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory, PartitioningUtils}
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

  def getPartitions(fileIndex: FileIndex,
                    partitionSpec: Option[TablePartitionSpec] = None): Seq[PartitionDirectory] = {
    val filters = if (partitionSpec.nonEmpty) {
      val partitionColumnsInfo: Map[String, DataType] =
        fileIndex.partitionSchema.map {
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
    } else {
      Nil
    }
    fileIndex.listFiles(filters)
  }

  /**
   * Get partition directory path(s) which has oap meta or data files,
   * return directories' paths if data is partitioned, or a single path if data is unpartitioned.
   * @param rootPaths the root paths of [[FileIndex]] of the relation
   * @param fs File system
   * @param partitionSchema partitioned column(s) schema of the relation
   * @param partitionSpec Schema of the partitioning columns,
   *                      or the empty schema if the table is not partitioned
   * @return all valid path(s) of directories containing meta or data files pertain to the table
   */
  def getPartitionPaths(
      rootPaths: Seq[Path],
      fs: FileSystem,
      partitionSchema: StructType,
      partitionSpec: Option[TablePartitionSpec] = None): Seq[Path] = {
    val directoryPaths =
      if (partitionSpec.nonEmpty) {
        val partitionAttributes = partitionSchema.map(field => (field.name, field.dataType)).toMap
        if (!partitionSpec.get.keys.forall(partitionAttributes.contains)) {
          throw new AnalysisException(
            s"""Partition spec is invalid. The spec (${partitionSpec.get.keys.mkString(", ")})
              |must match the partition spec (${partitionAttributes.mkString(", ")})""")
        }
        partitionSpec.get.foreach { case (attrName, value) =>
          val typeMatch = partitionAttributes(attrName) match {
            case StringType => scala.util.Try(value.toString)
            case IntegerType => scala.util.Try(value.toInt)
            case LongType => scala.util.Try(value.toLong)
            case BooleanType => scala.util.Try(value.toBoolean)
            case _: DataType =>
              throw new AnalysisException(
                s"Only handle partition key type in common use, check the partition key type:" +
                  s" ${partitionAttributes(attrName).toString}")
          }
          typeMatch match {
            case Failure(_) =>
              throw new AnalysisException(
                s"Type mismatch, value $value cannot convert to partition key type: " +
                  partitionAttributes(attrName).toString)
            case _ =>
          }
        }
        val pathFragment = PartitioningUtils.getPathFragment(partitionSpec.get, partitionSchema)
        rootPaths.map(rootPath => new Path(rootPath, pathFragment)).filter(fs.exists)
      } else {
        rootPaths
      }

    getPartitionPaths(directoryPaths, fs)
  }

  /**
   * Scan and Get the table's all directory path(s) which has oap meta file or data files
   * @param directoryPaths the input path(s) to search
   * @param fs File system
   * @return the table's all directory path(s) which has oap meta file or data files
   */
  private def getPartitionPaths(directoryPaths: Seq[Path], fs: FileSystem): Seq[Path] = {
    directoryPaths.filter(isTargetPath(_, fs)) ++
      fs.listStatus(directoryPaths.toArray[Path]).filter(_.isDirectory).flatMap { status =>
        getPartitionPaths(Seq(status.getPath), fs)
      }
  }

  /**
   * identify whether a path contains meta file or data files(s)
   * @param path the path to be checked
   * @param fs file system
   * @return true if the path directory contains meta or data file(s), otherwise false
   */
  private def isTargetPath(path: Path, fs: FileSystem): Boolean = {
    fs.exists(new Path(path, OapFileFormat.OAP_META_FILE)) ||
      fs.listStatus(path).filter(_.isFile).exists(status => isDataPath(status.getPath))
  }

  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
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

  /** Deprecated
   * Refresh any cached file listings of @param fileIndex,
   * and return partitions if data is partitioned, or a single partition if data is unpartitioned.
   * indicate all valid files grouped into partition(s) on the disk
   * @param fileIndex [[FileIndex]] of a relation
   * @param partitionSpec the specification of the partitions
   * @return all valid files grouped into partition(s) on the disk
   */
  @Deprecated
  // deprecated, use "getPartitionPaths" to get valid partition path(s)
  def getPartitionsRefreshed(
      fileIndex: FileIndex,
      partitionSpec: Option[TablePartitionSpec] = None): Seq[PartitionDirectory] = {
    fileIndex.refresh()
    getPartitions(fileIndex, partitionSpec)
  }
}
