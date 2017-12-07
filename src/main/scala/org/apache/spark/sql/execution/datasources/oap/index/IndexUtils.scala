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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.OutputStream

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


/**
 * Utils for Index read/write
 */
private[oap] object IndexUtils {

  def writeHead(writer: OutputStream, version: Int): Int = {
    val headerContent = "OAPIDX"
    writer.write(headerContent.getBytes("UTF-8"))
    assert(version <= 65535)
    val versionData = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(versionData)
    assert((headerContent.length + versionData.length) == IndexFile.indexFileHeaderLength)
    IndexFile.indexFileHeaderLength
  }

  def indexFileFromDataFile(dataFile: Path, name: String, time: String): Path = {
    import OapFileFormat._
    val dataFileName = dataFile.getName
    val pos = dataFileName.lastIndexOf(".")
    val indexFileName = if (pos > 0) {
      dataFileName.substring(0, pos)
    } else {
      dataFileName
    }
    new Path(
      dataFile.getParent, "." + indexFileName + "." + time + "." +  name + OAP_INDEX_EXTENSION)
  }

  def writeFloat(out: OutputStream, v: Float): Unit =
    writeInt(out, java.lang.Float.floatToIntBits(v))

  def writeDouble(out: OutputStream, v: Double): Unit =
    writeLong(out, java.lang.Double.doubleToLongBits(v))

  def writeBoolean(out: OutputStream, v: Boolean): Unit = out.write(if (v) 1 else 0)

  def writeByte(out: OutputStream, v: Int): Unit = out.write(v)

  def writeShort(out: OutputStream, v: Int): Unit = {
    out.write(v >>> 0 & 0XFF)
    out.write(v >>> 8 & 0xFF)
  }

  def writeInt(out: OutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
  }

  def writeLong(out: OutputStream, v: Long): Unit = {
    out.write((v >>>  0).toInt & 0xFF)
    out.write((v >>>  8).toInt & 0xFF)
    out.write((v >>> 16).toInt & 0xFF)
    out.write((v >>> 24).toInt & 0xFF)
    out.write((v >>> 32).toInt & 0xFF)
    out.write((v >>> 40).toInt & 0xFF)
    out.write((v >>> 48).toInt & 0xFF)
    out.write((v >>> 56).toInt & 0xFF)
  }

  /**
   * Note: outputPath comes from `FileOutputFormat.getOutputPath`, which is made by Data source
   * API, so `outputPath` should be simple enough, without scheme and authority.
   */
  def getIndexWorkPath(
      inputFile: Path, outputPath: Path, attemptPath: Path, indexFile: String): Path = {
    new Path(inputFile.getParent.toString.replace(
      outputPath.toString, attemptPath.toString), indexFile)
  }

  def readBasedOnDataType(
      fiberCache: FiberCache, offset: Long, dataType: DataType): (Any, Long) = {
    dataType match {
      case BooleanType => (fiberCache.getBoolean(offset), BooleanType.defaultSize)
      case ByteType => (fiberCache.getByte(offset), ByteType.defaultSize)
      case ShortType => (fiberCache.getShort(offset), ShortType.defaultSize)
      case IntegerType => (fiberCache.getInt(offset), IntegerType.defaultSize)
      case LongType => (fiberCache.getLong(offset), LongType.defaultSize)
      case FloatType => (fiberCache.getFloat(offset), FloatType.defaultSize)
      case DoubleType => (fiberCache.getDouble(offset), DoubleType.defaultSize)
      case DateType => (fiberCache.getInt(offset), DateType.defaultSize)
      case StringType =>
        val length = fiberCache.getInt(offset)
        val string = fiberCache.getUTF8String(offset + INT_SIZE, length)
        (string, INT_SIZE + length)
      case BinaryType =>
        val length = fiberCache.getInt(offset)
        val bytes = fiberCache.getBytes(offset + INT_SIZE, length)
        (bytes, INT_SIZE + bytes.length)
      case other => throw new OapException(s"OAP index currently doesn't support data type $other")
    }
  }

  def readBasedOnSchema(
      fiberCache: FiberCache, offset: Long, schema: StructType): InternalRow = {
    var pos = offset
    val values = schema.map(_.dataType).map { dataType =>
      val (value, length) = readBasedOnDataType(fiberCache, pos, dataType)
      pos += length
      value
    }
    InternalRow.fromSeq(values)
  }

  def writeBasedOnDataType(
      out: OutputStream,
      value: Any): Unit = {
    value match {
      case null => throw new OapException(s"trying to write null key!")
      case boolean: Boolean => writeBoolean(out, boolean)
      case short: Short => writeShort(out, short)
      case byte: Byte => writeByte(out, byte)
      case int: Int => writeInt(out, int)
      case long: Long => writeLong(out, long)
      case float: Float => writeFloat(out, float)
      case double: Double => writeDouble(out, double)
      case string: UTF8String =>
        val bytes = string.getBytes
        writeInt(out, bytes.length)
        out.write(bytes)
      case binary: Array[Byte] =>
        writeInt(out, binary.length)
        out.write(binary)
      case other => throw new OapException(s"OAP index currently doesn't support data type $other")
    }
  }

  def writeBasedOnSchema(
      out: OutputStream, row: InternalRow, schema: StructType): Unit = {
    require(row != null)
    schema.zipWithIndex.foreach {
      case (field, index) => writeBasedOnDataType(out, row.get(index, field.dataType))
    }
  }

  val INT_SIZE = 4
  val LONG_SIZE = 8
}
