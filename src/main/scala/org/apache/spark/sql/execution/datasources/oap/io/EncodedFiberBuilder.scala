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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.{PlainBinaryDictionaryValuesWriter, PlainIntegerDictionaryValuesWriter}
import org.apache.parquet.format.Encoding
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache.{DataFiberBuilder, FiberByteData}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StringType}
import org.apache.spark.unsafe.Platform

private[oap] case class DeltaByteArrayFiberBuilder (
  defaultRowGroupRowCount: Int,
  ordinal: Int,
  dataType: DataType) extends DataFiberBuilder {

  // TODO: [linhong] hard-coded variables need to remove
  private val valuesWriter = new DeltaByteArrayWriter(32, 1048576)
  private var dataLengthInBytes: Int = _

  override def getEncoding: Encoding = Encoding.DELTA_BYTE_ARRAY

  override protected def appendInternal(row: InternalRow): Unit = {

    val value = dataType match {
      case StringType => Binary.fromConstantByteArray(row.getUTF8String(ordinal).getBytes)
      case BinaryType => Binary.fromConstantByteArray(row.getBinary(ordinal))
      case _ => sys.error(s"Not support data type: $dataType")
    }
    valuesWriter.writeBytes(value)

    dataLengthInBytes += value.getBytes.length
  }

  override def build(): FiberByteData = {

    val bits = new Array[Byte](bitStream.toLongArray().length * 8)

    Platform.copyMemory(bitStream.toLongArray(), Platform.LONG_ARRAY_OFFSET,
      bits, Platform.BYTE_ARRAY_OFFSET, bitStream.toLongArray().length * 8)

    val bytes = BytesInput.concat(BytesInput.from(bits),
      BytesInput.fromInt(dataLengthInBytes),
      valuesWriter.getBytes).toByteArray

    FiberByteData(bytes)
  }

  override def clear(): Unit = {

    super.clear()
    valuesWriter.reset()
    dataLengthInBytes = 0
  }
}

// TODO: [linhong] Code is similar to DeltaByteArrayFiberBuilder. Need abstract
private[oap] case class PlainBinaryDictionaryFiberBuilder(
  defaultRowGroupRowCount: Int,
  ordinal: Int,
  dataType: DataType) extends DataFiberBuilder {

  private val valuesWriter = new PlainBinaryDictionaryValuesWriter(1048576,
    org.apache.parquet.column.Encoding.RLE_DICTIONARY,
    org.apache.parquet.column.Encoding.PLAIN)

  private var dataLengthInBytes: Int = _

  override def getEncoding: Encoding = Encoding.PLAIN_DICTIONARY

  override protected def appendInternal(row: InternalRow) = {

    val value = dataType match {
      case StringType => Binary.fromConstantByteArray(row.getUTF8String(ordinal).getBytes)
      case BinaryType => Binary.fromConstantByteArray(row.getBinary(ordinal))
      case _ => sys.error(s"Not support data type: $dataType")
    }
    valuesWriter.writeBytes(value)

    dataLengthInBytes += value.getBytes.length
  }

  override def build(): FiberByteData = {

    val bits = new Array[Byte](bitStream.toLongArray().length * 8)

    Platform.copyMemory(bitStream.toLongArray(), Platform.LONG_ARRAY_OFFSET,
      bits, Platform.BYTE_ARRAY_OFFSET, bitStream.toLongArray().length * 8)

    val bytes = BytesInput.concat(BytesInput.from(bits),
      BytesInput.fromInt(dataLengthInBytes),
      valuesWriter.getBytes).toByteArray

    FiberByteData(bytes)
  }

  override def buildDictionary: Array[Byte] = {
    val dictionary = valuesWriter.createDictionaryPage()
    if (dictionary != null) {
      dictionary.getBytes.toByteArray
    } else {
      Array.empty[Byte]
    }
  }

  override def getDictionarySize: Int = valuesWriter.getDictionarySize

  override def clear(): Unit = {
    super.clear()
    valuesWriter.reset()
    dataLengthInBytes = 0
  }
}

private[oap] case class PlainIntegerDictionaryFiberBuilder(
  defaultRowGroupRowCount: Int,
  ordinal: Int,
  dataType: DataType) extends DataFiberBuilder {

  private val valuesWriter = new PlainIntegerDictionaryValuesWriter(1048576,
    org.apache.parquet.column.Encoding.RLE_DICTIONARY,
    org.apache.parquet.column.Encoding.PLAIN)

  private var dataLengthInBytes: Int = _

  override def getEncoding: Encoding = Encoding.PLAIN_DICTIONARY

  override protected def appendInternal(row: InternalRow) = {

    val value = dataType match {
      case IntegerType => row.getInt(ordinal)
      case _ => sys.error(s"Not support data type: $dataType")
    }
    valuesWriter.writeInteger(value)
  }

  override def build(): FiberByteData = {

    val bits = new Array[Byte](bitStream.toLongArray().length * 8)

    Platform.copyMemory(bitStream.toLongArray(), Platform.LONG_ARRAY_OFFSET,
      bits, Platform.BYTE_ARRAY_OFFSET, bitStream.toLongArray().length * 8)

    val bytes = BytesInput.concat(BytesInput.from(bits),
      BytesInput.fromInt(currentRowId),
      valuesWriter.getBytes).toByteArray

    FiberByteData(bytes)
  }

  override def buildDictionary: Array[Byte] = {
    val dictionary = valuesWriter.createDictionaryPage()
    if (dictionary != null) {
      dictionary.getBytes.toByteArray
    } else {
      Array.empty[Byte]
    }
  }

  override def getDictionarySize: Int = valuesWriter.getDictionarySize

  override def clear(): Unit = {
    super.clear()
    valuesWriter.reset()
    dataLengthInBytes = 0
  }
}
