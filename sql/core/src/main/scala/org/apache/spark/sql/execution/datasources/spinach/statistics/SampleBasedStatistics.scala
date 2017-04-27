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
package org.apache.spark.sql.execution.datasources.spinach.statistics

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.index.RangeInterval
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

class SampleBasedStatistics(var content: Seq[StatisticsEntry] = null,
                            sampleRate: Double = 0.1) extends Statistics {
  override val id: Int = 1
  override var arrayOffset: Long = _

  // TODO refactor offset variable to provide an easy access to file offset
  override def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
                    stsArray: Array[Byte], offset_temp: Long): Double = {
    var offset = offset_temp
    val id_from_file = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset)
    offset += 4
    assert(id_from_file == id, "Statistics type mismatch")
    val ordering = GenerateOrdering.create(schema)
    val size_from_file = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset)
    offset += 4

    var hit_count = 0
    for (_ <- 0 until size_from_file) {
      // read UnsafeRow, calculate hit_count without storing a single row
      val size = Platform.getInt(stsArray, Platform.BYTE_ARRAY_OFFSET + offset)
      val row = Statistics.getUnsafeRow(schema.length, stsArray, offset + 4, size)
      val off = Platform.getLong(stsArray, Platform.BYTE_ARRAY_OFFSET + offset) // redundant offset
      offset = offset + 12 + size

      if (Statistics.rowInIntervalArray(row, intervalArray, ordering)) hit_count += 1
    }
    arrayOffset = offset
    hit_count * 1.0 / size_from_file
  }
}
