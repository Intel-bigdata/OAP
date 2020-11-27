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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime

object CachedPartitionedFileUtil {
  def splitFilesWithCacheLocality(
      file: FileStatus,
      filePath: Path,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    Seq(getPartitionedFile(file, filePath, partitionValues))
  }

  def getPartitionedFile(
      file: FileStatus,
      filePath: Path,
      partitionValues: InternalRow): PartitionedFile = {
    val cachedHosts =
      if (!SparkEnv.get.conf.get(OapConf.OAP_EXTERNAL_CACHE_METADB_ENABLED)) {
        OapRuntime.getOrCreate.fiberSensor.getHosts(file.getPath.toString).toArray
      } else {
        new Array[String](0)
      }
    val hosts = cachedHosts ++ getBlockHosts(getBlockLocations(file), file.getLen)
    PartitionedFile(partitionValues, filePath.toUri.toString, 0, file.getLen, hosts)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case _ => Array.empty[BlockLocation]
  }
  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
      blockLocations: Array[BlockLocation],
      length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block. It handles the case where the
      // fragment is fully contained in the block.
      case b if b.getOffset == 0 && 0 < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength).min(length)

      // The fragment ends at a position within this block
      case b if b.getOffset < length && length < b.getOffset + b.getLength =>
        b.getHosts -> (length - b.getOffset)

      // The fragment fully contains this block
      case b if 0 <= b.getOffset && b.getOffset + b.getLength <= length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (_, size) =>
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
