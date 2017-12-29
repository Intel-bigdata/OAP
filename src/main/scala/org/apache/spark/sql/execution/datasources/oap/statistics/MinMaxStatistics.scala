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

package org.apache.spark.sql.execution.datasources.oap.statistics

import java.io.{ByteArrayOutputStream, OutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.types.StructType


private[oap] class MinMaxStatisticsReader(schema: StructType) extends StatisticsReader(schema) {
  override val id: Int = StatisticsType.TYPE_MIN_MAX

  protected var min: Key = _
  protected var max: Key = _

  override def read(fiberCache: FiberCache, offset: Int): Int = {
    var readOffset = super.read(fiberCache, offset) + offset // offset after super.read

    val minSize = fiberCache.getInt(readOffset)
    readOffset += 4
    if (minSize != 0) {
      val totalSize = fiberCache.getInt(readOffset)
      readOffset += 4
      min = nnkr.readKey(fiberCache, readOffset)._1
      max = nnkr.readKey(fiberCache, readOffset + minSize)._1
      readOffset += totalSize
    }

    readOffset - offset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {
    if (min == null || max == null) return StaticsAnalysisResult.USE_INDEX

    val start = intervalArray.head
    val end = intervalArray.last

    val startOrdering = GenerateOrdering.create(StructType(schema.slice(0, start.start.numFields)))
    val endOrdering = GenerateOrdering.create(StructType(schema.slice(0, end.end.numFields)))

    val startOutOfBound =
      if (start.start.numFields == schema.length && !start.startInclude) {
        startOrdering.gteq(start.start, max)
      } else startOrdering.gt(start.start, max)

    val endOutOfBound =
      if (end.end.numFields == schema.length && !end.endInclude) {
        endOrdering.lteq(end.end, min)
      } else endOrdering.lt(end.end, min)

    if (startOutOfBound || endOutOfBound) StaticsAnalysisResult.SKIP_INDEX
    else StaticsAnalysisResult.USE_INDEX
  }
}

private[oap] class MinMaxStatisticsWriter(
    schema: StructType, conf: Configuration) extends StatisticsWriter(schema, conf) {
  override val id: Int = StatisticsType.TYPE_MIN_MAX
  @transient
  private lazy val ordering = GenerateOrdering.create(schema)

  protected var min: Key = _
  protected var max: Key = _

  override def addOapKey(key: Key): Unit = {
    if (min == null || max == null) {
      min = key
      max = key
    } else {
      if (ordering.compare(key, min) < 0) min = key
      if (ordering.compare(key, max) > 0) max = key
    }
  }

  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Int = {
    var offset = super.write(writer, sortedKeys)
    if (min != null) {
      val tempWriter = new ByteArrayOutputStream()
      nnkw.writeKey(tempWriter, min)
      IndexUtils.writeInt(writer, tempWriter.size)
      nnkw.writeKey(tempWriter, max)
      IndexUtils.writeInt(writer, tempWriter.size)
      offset += IndexUtils.INT_SIZE * 2
      writer.write(tempWriter.toByteArray)
      offset += tempWriter.size
    } else {
      // No valid min/max.
      IndexUtils.writeInt(writer, 0)
      offset += IndexUtils.INT_SIZE
    }
    offset
  }
}
