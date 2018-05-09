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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.types.StructType

case class OapFullScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    outputSchema: StructType)
  extends LeafExecNode {

  @transient private lazy val selectedPartitions = relation.location.listFiles(Nil)

  override protected def doExecute(): RDD[InternalRow] = {

    val files = selectedPartitions.flatMap { partition =>
      partition.files.map { file =>
        val blockLocations = getBlockLocations(file)
        val cachedHosts = FiberSensor.getHosts(file.getPath.toString)
        val hosts = cachedHosts.toBuffer ++ getBlockHosts(blockLocations, 0, file.getLen)
        logWarning("hosts: " + hosts.mkString(","))
        PartitionedFile(
          partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts.toArray)
      }
    }

    val partitions = files.indices.map (i => FilePartition(i, Seq(files(i))))

    new OapFullScanRDD(relation.sparkSession, partitions, outputSchema)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  private def getBlockHosts(
      blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}
