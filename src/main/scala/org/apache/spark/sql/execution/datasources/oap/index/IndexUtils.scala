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
import org.apache.parquet.bytes.LittleEndianDataOutputStream

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

  def writeInt(out: OutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
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

  def writeLong(writer: OutputStream, v: Long): Unit = {
    writer.write((v >>>  0).toInt & 0xFF)
    writer.write((v >>>  8).toInt & 0xFF)
    writer.write((v >>> 16).toInt & 0xFF)
    writer.write((v >>> 24).toInt & 0xFF)
    writer.write((v >>> 32).toInt & 0xFF)
    writer.write((v >>> 40).toInt & 0xFF)
    writer.write((v >>> 48).toInt & 0xFF)
    writer.write((v >>> 56).toInt & 0xFF)
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

  def writeBasedOnDataType(
      writer: LittleEndianDataOutputStream,
      value: Any): Unit = {
    value match {
      case int: Boolean => writer.writeBoolean(int)
      case short: Short => writer.writeShort(short)
      case byte: Byte => writer.writeByte(byte)
      case int: Int => writer.writeInt(int)
      case long: Long => writer.writeLong(long)
      case float: Float => writer.writeFloat(float)
      case double: Double => writer.writeDouble(double)
      case string: UTF8String =>
        val bytes = string.getBytes
        writer.writeInt(bytes.length)
        writer.write(bytes)
      case binary: Array[Byte] =>
        writer.writeInt(binary.length)
        writer.write(binary)
      case other => throw new OapException(s"OAP index currently doesn't support data type $other")
    }
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
        val string = fiberCache.getUTF8String(offset + Integer.SIZE / 8, length)
        (string, Integer.SIZE / 8 + length)
      case BinaryType =>
        val length = fiberCache.getInt(offset)
        val bytes = fiberCache.getBytes(offset + Integer.SIZE / 8, length)
        (bytes, Integer.SIZE / 8 + bytes.length)
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

  def writeBasedOnSchema(
      writer: LittleEndianDataOutputStream, row: InternalRow, schema: StructType): Unit = {
    require(row != null)
    schema.zipWithIndex.foreach {
      case (field, index) => writeBasedOnDataType(writer, row.get(index, field.dataType))
    }
  }
}
