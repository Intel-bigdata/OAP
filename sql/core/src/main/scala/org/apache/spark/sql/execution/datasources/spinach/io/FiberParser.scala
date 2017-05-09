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

package org.apache.spark.sql.execution.datasources.spinach.io

import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader
import org.apache.parquet.format.Encoding

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.BitSet

private[spinach] trait DataFiberParser {
  def parse(bytes: Array[Byte], rowCount: Int): Array[Byte]
}

object DataFiberParser {
  def apply(encoding: Encoding,
            meta: SpinachDataFileHandle,
            dataType: DataType): DataFiberParser = {

    encoding match {
      case Encoding.PLAIN => PlainDataFiberParser(meta)
      case Encoding.DELTA_BYTE_ARRAY => DeltaByteArrayDataFiberParser(meta, dataType)
      case _ => sys.error(s"Not support encoding type: $encoding")
    }
  }
}

private[spinach] case class PlainDataFiberParser(
  meta: SpinachDataFileHandle) extends DataFiberParser{

  override def parse(bytes: Array[Byte], rowCount: Int): Array[Byte] = bytes
}
private[spinach] case class DeltaByteArrayDataFiberParser(
  meta: SpinachDataFileHandle, dataType: DataType) extends DataFiberParser{


  override def parse(bytes: Array[Byte], rowCount: Int): Array[Byte] = {

    val valuesReader = new DeltaByteArrayReader()

    val bits = new BitSet(meta.rowCountInEachGroup)
    Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET,
      bits.toLongArray(), Platform.LONG_ARRAY_OFFSET, bits.toLongArray().length * 8)

    val baseOffset = Platform.BYTE_ARRAY_OFFSET + bits.toLongArray().length * 8
    val bitsDataLength = bits.toLongArray().length * 8
    val valueDataLength = Platform.getInt(bytes, baseOffset)

    dataType match {
      case BinaryType | StringType =>
        // 2 Integers for each String to indicate start offset and length
        // TODO: [linhong] 2 Integers are redundant
        val offsetDataLength = rowCount * IntegerType.defaultSize * 2
        var startValueOffset = bitsDataLength + offsetDataLength

        val fiberBytesLength = bitsDataLength + offsetDataLength + valueDataLength
        val fiberBytes = new Array[Byte](fiberBytesLength)

        Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET,
          fiberBytes, Platform.LONG_ARRAY_OFFSET, bits.toLongArray().length * 8)

        valuesReader.initFromPage(rowCount, bytes, bitsDataLength + 4)

        (0 until rowCount).foreach{i =>
          if (bits.get(i)) {
            val value = valuesReader.readBytes().getBytes
            Platform.putInt(fiberBytes,
              baseOffset + IntegerType.defaultSize * i * 2, value.length)
            Platform.putInt(fiberBytes,
              baseOffset + IntegerType.defaultSize * (i * 2 + 1), startValueOffset)
            Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, fiberBytes,
              Platform.BYTE_ARRAY_OFFSET + startValueOffset, value.length)
            startValueOffset += value.length
          }
        }
        fiberBytes
      case _ => sys.error(s"Not support data type: $dataType")
    }
  }
}
