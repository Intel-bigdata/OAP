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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

private[oap] class MinMaxStatistics extends Statistics {
  override val id: Int = MinMaxStatisticsType.id
  @transient private lazy val converter = UnsafeProjection.create(schema)
  @transient private lazy val ordering = GenerateOrdering.create(schema)

  protected var min: Key = _
  protected var max: Key = _

  override def initialize(schema: StructType): Unit = {
    super.initialize(schema)
    min = null
    max = null
  }

  override def addOapKey(key: Key): Unit = {
    if (min == null || max == null) {
      min = key
      max = key
    } else {
      if (ordering.compare(key, min) < 0) min = key
      if (ordering.compare(key, max) > 0) max = key
    }
  }

  override def write(writer: IndexOutputWriter, sortedKeys: ArrayBuffer[Key]): Long = {
    var offset = super.write(writer, sortedKeys)
    if (min != null) {
      offset += Statistics.writeInternalRow(converter, min, writer)
      offset += Statistics.writeInternalRow(converter, max, writer)
    }
    offset
  }

  override def read(bytes: Array[Byte], baseOffset: Long): Long = {
    var offset = super.read(bytes, baseOffset) + baseOffset // offset after super.read

    val minSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    min = Statistics.getUnsafeRow(schema.length, bytes, offset, minSize).copy()
    offset += (4 + minSize)

    val maxSize = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    max = Statistics.getUnsafeRow(schema.length, bytes, offset, maxSize).copy()
    offset += (4 + maxSize)

    offset - baseOffset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {
    val start = intervalArray.head
    val end = intervalArray.last

    val startOutOfBound =
      if (start.start.numFields > 0) {
        val startSchema = StructType(schema.slice(0, start.start.numFields))
        val startOrdering = GenerateOrdering.create(startSchema)
        if (start.start.numFields == schema.length && !start.startInclude) {
          startOrdering.gteq(start.start, max)
        } else startOrdering.gt(start.start, max)
      } else false

    val endOutOfBound =
      if (end.end.numFields > 0) {
        val endSchema = StructType(schema.slice(0, end.end.numFields))
        val endOrdering = GenerateOrdering.create(endSchema)
        if (end.end.numFields == schema.length && !end.endInclude) {
          endOrdering.lteq(end.end, min)
        } else endOrdering.lt(end.end, min)
      } else false

    if (startOutOfBound || endOutOfBound) StaticsAnalysisResult.SKIP_INDEX
    else StaticsAnalysisResult.USE_INDEX
  }
}
