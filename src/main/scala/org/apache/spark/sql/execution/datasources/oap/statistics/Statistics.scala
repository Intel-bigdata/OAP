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

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.BaseOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

abstract class Statistics{
  val id: Int
  protected var schema: StructType = _

  def initialize(schema: StructType): Unit = {
    this.schema = schema
  }

  /**
   * For MinMax & Bloom Filter, every time a key is inserted, then
   * the info should be updated, for SampleBase and PartByValue statistics,
   * nothing done in this function, a in-memory key array is stored in
   * `StatisticsManager`. This function does nothing for most cases.
   * @param key an InternalRow from index partition
   */
  def addOapKey(key: Key): Unit = {
  }

  /**
   * Statistics write function, by default, only a Statistics id should be
   * written into the writer.
   * @param writer IndexOutputWrite, where to write the information
   * @param sortedKeys sorted keys stored related to this statistics
   * @return number of bytes written in writer
   */
  def write(writer: IndexOutputWriter, sortedKeys: ArrayBuffer[Key]): Long = {
    IndexUtils.writeInt(writer, id)
    4L
  }

  /**
   * Statistics read function, by default, statistics id should be same with
   * current statistics
   * @param bytes bytes read from file
   * @param baseOffset start offset to read the statistics
   * @return number of bytes read from `bytes` array
   */
  def read(bytes: Array[Byte], baseOffset: Long): Long = {
    val idFromFile = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + baseOffset)
    assert(idFromFile == id)
    4L
  }

  /**
   * Analyse the query `intervalArray` with `Statistics`, by default, if no content
   * is in this Statistics, then we should use index for the correctness of this array.
   * @param intervalArray query intervals from `IndexContext`
   * @return the `StaticsAnalysisResult`
   */
  def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double =
    StaticsAnalysisResult.USE_INDEX
}

// tool function for Statistics class
object Statistics {
  def getUnsafeRow(schemaLen: Int, array: Array[Byte], offset: Long, size: Int): UnsafeRow = {
    UnsafeIndexNode.getUnsafeRow(schemaLen, array, Platform.BYTE_ARRAY_OFFSET + offset + 4, size)
  }

  /**
   * This method help oap convert InternalRow type to UnsafeRow type
   * @param internalRow
   * @param keyBuf
   * @return unsafeRow
   */
  def convertHelper(converter: UnsafeProjection,
                    internalRow: InternalRow,
                    keyBuf: ByteArrayOutputStream): UnsafeRow = {
    converter.apply(internalRow)
  }

  def writeInternalRow(converter: UnsafeProjection,
                       internalRow: InternalRow,
                       writer: IndexOutputWriter): Int = {
    val keyBuf = new ByteArrayOutputStream()
    val value = convertHelper(converter, internalRow, keyBuf)

    IndexUtils.writeInt(keyBuf, value.getSizeInBytes)
    value.writeToStream(keyBuf, null)
    writer.write(keyBuf.toByteArray)
    keyBuf.close()
    4 + value.getSizeInBytes
  }

  // logic is complex, needs to be refactored :(
  def rowInSingleInterval(row: InternalRow, interval: RangeInterval,
                          startOrder: BaseOrdering, endOrder: BaseOrdering): Boolean = {
    // Only two cases are accepted, or something is wrong.
    // 1. row = [1, "aaa"], start = [1, "bbb"] => row.numFields == start.numFields
    // 2. row = [1, "aaa"], start = [1, DUMMY_KEY_START] => row.numFields -1 = start.numFields
    assert(interval.start.numFields == row.numFields ||
      interval.start.numFields == row.numFields - 1,
      s"Can't compare row with interval.start. row: $row, interval: ${interval.start}")

    assert(interval.end.numFields == row.numFields ||
      interval.end.numFields == row.numFields - 1,
      s"Can't compare row with interval.end. row: $row, interval: ${interval.end}")

    val withinStart =
      if (row.numFields == interval.start.numFields && !interval.startInclude) {
        startOrder.compare(row, interval.start) > 0
      } else {
        startOrder.compare(row, interval.start) >= 0
      }
    val withinEnd =
      if (row.numFields == interval.end.numFields && !interval.endInclude) {
        endOrder.compare(row, interval.end) < 0
      } else {
        endOrder.compare(row, interval.end) <= 0
      }
    withinStart && withinEnd
  }

  def rowInIntervalArray(row: InternalRow, intervalArray: ArrayBuffer[RangeInterval],
                         fullOrder: BaseOrdering, partialOrder: BaseOrdering): Boolean = {
    if (intervalArray == null || intervalArray.isEmpty) false
    else intervalArray.exists{interval =>
      val startOrder =
        if (interval.start.numFields == row.numFields) fullOrder else partialOrder
      val endOrder =
        if (interval.end.numFields == row.numFields) fullOrder else partialOrder
      rowInSingleInterval(row, interval, startOrder, endOrder)}
  }
}

/**
 * StaticsAnalysisResult should be one of these following values,
 * or a double value between 0 and 1, standing for the estimated
 * coverage form this content.
 */
object StaticsAnalysisResult {
  val FULL_SCAN = 1
  val SKIP_INDEX = -1
  val USE_INDEX = 0
}
