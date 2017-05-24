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

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.BaseOrdering
import org.apache.spark.sql.execution.datasources.spinach.Key
import org.apache.spark.sql.execution.datasources.spinach.index._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform


abstract class Statistics{
  val id: Int
  var arrayOffset: Long
  protected var schema: StructType = _

  def initialize(schema: StructType): Unit = {
    this.schema = schema
  }

  def addSpinachKey(key: Key): Unit = { // spinach.Key == InternalRow
    // do nothing for most of cases
  }

  // return: number of bytes written into writer
  def write(writer: IndexOutputWriter, sortedKeys: ArrayBuffer[Key]): Long = {
    IndexUtils.writeInt(writer, id)
    4L
  }

  // return number of bytes read from `bytes` array
  def read(bytes: Array[Byte], baseOffset: Long): Long = {
    val idFromFile = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + baseOffset)
    assert(idFromFile == id)
    4L
  }

  def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double =
    StaticsAnalysisResult.USE_INDEX

  // TODO write function parameters need to be optimized
  def write(schema: StructType, writer: IndexOutputWriter, uniqueKeys: Array[InternalRow],
            hashMap: java.util.HashMap[InternalRow, java.util.ArrayList[Long]],
            offsetMap: java.util.HashMap[InternalRow, Long]): Unit

  // TODO parameter needs to be optimized
  def read(schema: StructType, intervalArray: ArrayBuffer[RangeInterval],
           stsArray: Array[Byte], offset: Long): Double
}


object Statistics {
  def getUnsafeRow(schemaLen: Int, array: Array[Byte], offset: Long, size: Int): UnsafeRow = {
    UnsafeIndexNode.getUnsafeRow(schemaLen, array, Platform.BYTE_ARRAY_OFFSET + offset + 4, size)
  }

  /**
   * This method help spinach convert InternalRow type to UnsafeRow type
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
  def rowInSingleInterval(row: UnsafeRow, interval: RangeInterval,
                          order: BaseOrdering): Boolean = {
    if (interval.start == IndexScanner.DUMMY_KEY_START) {
      if (interval.end == IndexScanner.DUMMY_KEY_END) true
      else {
        if (order.lt(row, interval.end)) true
        else if (order.equiv(row, interval.end) && interval.endInclude) true
        else false
      }
    } else {
      if (order.lt(row, interval.start)) false
      else if (order.equiv(row, interval.start)) {
        if (interval.startInclude) {
          if (interval.end != IndexScanner.DUMMY_KEY_END &&
            order.equiv(interval.start, interval.end) && !interval.endInclude) {false}
          else true
        } else interval.end == IndexScanner.DUMMY_KEY_END || order.gt(interval.end, row)
      }
      else if (interval.end != IndexScanner.DUMMY_KEY_END && (order.gt(row, interval.end) ||
        (order.equiv(row, interval.end) && !interval.endInclude))) {false}
      else true
    }
  }
  def rowInIntervalArray(row: UnsafeRow, intervalArray: ArrayBuffer[RangeInterval],
                         order: BaseOrdering): Boolean = {
    if (intervalArray == null || intervalArray.isEmpty) false
    else intervalArray.exists(interval => rowInSingleInterval(row, interval, order))
  }
}

object StaticsAnalysisResult {
  val FULL_SCAN = 1
  val SKIP_INDEX = -1
  val USE_INDEX = 0
}
