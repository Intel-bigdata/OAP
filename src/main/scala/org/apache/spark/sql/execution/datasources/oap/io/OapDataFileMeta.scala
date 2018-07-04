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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.parquet.column.statistics._
import org.apache.parquet.format.{CompressionCodec, Encoding}

import org.apache.spark.sql.types._

//  OAP Data File V1 Meta Part
//  ..
//  Field                               Length In Byte
//  Meta
//    Magic and Version                 4
//    Row Count In Each Row Group       4
//    Row Count In Last Row Group       4
//    Row Group Count                   4
//    Field Count In each Row           4
//    Compression Codec                 4
//    Column Meta #1                    4 * 5 + Length of Min Max Value Data
//      Encoding                        4
//      Dictionary Data Length          4
//      Dictionary Id Size              4
//      Min Value Data Length           4
//      Min Value Data                  Min Value Data Length
//      Max Value Data Length           4
//      Max Value Data                  Max Value Data Length
//    Column Meta #2
//    ..
//    Column Meta #N
//    RowGroup Meta #1                  16 + 4 * Field Count In Each Row * 2
//      RowGroup StartPosition          8
//      RowGroup EndPosition            8
//      Fiber #1 Length (Compressed)    4
//      Fiber #2 Length (Compressed)    4
//      ...                             4
//      Fiber #N Length (Compressed)    4
//      Fiber #1 Uncompressed Length    4
//      Fiber #2 Uncompressed Length    4
//      ...                             4
//      Fiber #N Uncompressed Length    4
//    RowGroup Meta #2                  16 + 4 * Field Count In Each Row * 2
//    RowGroup Meta #3                  16 + 4 * Field Count In Each Row * 2
//    ..                                16 + 4 * Field Count In Each Row * 2
//    RowGroup Meta #N                  16 + 4 * Field Count In Each Row * 2
//    Meta Data Length                  4

private[oap] class RowGroupMeta {
  var start: Long = _
  var end: Long = _
  var fiberLens: Array[Int] = _
  var fiberUncompressedLens: Array[Int] = _
  var statistics: Array[ColumnStatistics] = _

  def withNewStart(newStart: Long): RowGroupMeta = {
    this.start = newStart
    this
  }

  def withNewEnd(newEnd: Long): RowGroupMeta = {
    this.end = newEnd
    this
  }

  def withNewFiberLens(newFiberLens: Array[Int]): RowGroupMeta = {
    this.fiberLens = newFiberLens
    this
  }

  def withNewUncompressedFiberLens(newUncompressedFiberLens: Array[Int]): RowGroupMeta = {
    this.fiberUncompressedLens = newUncompressedFiberLens
    this
  }

  def withNewStatistics(newStatistics: Array[ColumnStatistics]): RowGroupMeta = {
    this.statistics = newStatistics
    this
  }

  def write(os: FSDataOutputStream): RowGroupMeta = {
    os.writeLong(start)
    os.writeLong(end)
    fiberLens.foreach(os.writeInt)
    fiberUncompressedLens.foreach(os.writeInt)
    statistics.foreach {
      case ColumnStatistics(bytes) => os.write(bytes)
    }
    this
  }

  def read(is: DataInputStream, fieldCount: Int): RowGroupMeta = {
    start = is.readLong()
    end = is.readLong()
    fiberLens = new Array[Int](fieldCount)
    fiberUncompressedLens = new Array[Int](fieldCount)

    fiberLens.indices.foreach(fiberLens(_) = is.readInt())
    fiberUncompressedLens.indices.foreach(fiberUncompressedLens(_) = is.readInt())
    statistics = new Array[ColumnStatistics](fieldCount)
    statistics.indices.foreach(statistics(_) = ColumnStatistics(is))
    this
  }
}

private[oap] class ColumnStatistics(val min: Array[Byte], val max: Array[Byte]) {
  def hasNonNullValue: Boolean = min != null && max != null

  def isEmpty: Boolean = !hasNonNullValue
}

private[oap] object ColumnStatistics {

  type ParquetStatistics = org.apache.parquet.column.statistics.Statistics[_ <: Comparable[_]]

  def getStatsBasedOnType(dataType: DataType): ParquetStatistics = {
    dataType match {
      case BooleanType => new BooleanStatistics()
      case IntegerType | ByteType | DateType | ShortType => new IntStatistics()
      case StringType | BinaryType => new BinaryStatistics()
      case FloatType => new FloatStatistics()
      case DoubleType => new DoubleStatistics()
      case LongType => new LongStatistics()
      case _ => sys.error(s"Not support data type: $dataType")
    }
  }

  def getStatsFromSchema(schema: StructType): Seq[ParquetStatistics] = {
    schema.map{ field => getStatsBasedOnType(field.dataType)}
  }

  def apply(stat: ParquetStatistics): ColumnStatistics = {
    if (!stat.hasNonNullValue) {
      new ColumnStatistics(null, null)
    } else {
      new ColumnStatistics(stat.getMinBytes, stat.getMaxBytes)
    }
  }

  def apply(in: DataInputStream): ColumnStatistics = {

    val minLength = in.readInt()
    val min = if (minLength != 0) {
      val bytes = new Array[Byte](minLength)
      in.readFully(bytes)
      bytes
    } else {
      null
    }

    val maxLength = in.readInt()
    val max = if (maxLength != 0) {
      val bytes = new Array[Byte](maxLength)
      in.readFully(bytes)
      bytes
    } else {
      null
    }

    new ColumnStatistics(min, max)
  }

  def unapply(statistics: ColumnStatistics): Option[Array[Byte]] = {
    val buf = new ByteArrayOutputStream()
    val out = new DataOutputStream(buf)

    if (statistics.hasNonNullValue) {
      out.writeInt(statistics.min.length)
      out.write(statistics.min)
      out.writeInt(statistics.max.length)
      out.write(statistics.max)
    } else {
      out.writeInt(0)
      out.writeInt(0)
    }

    Some(buf.toByteArray)
  }
}

private[oap] class ColumnMeta(
    val encoding: Encoding,
    val dictionaryDataLength: Int,
    val dictionaryIdSize: Int,
    val fileStatistics: ColumnStatistics) {}

private[oap] object ColumnMeta {

  def apply(in: DataInputStream): ColumnMeta = {

    val encoding = Encoding.findByValue(in.readInt())
    val dictionaryDataLength = in.readInt()
    val dictionaryIdSize = in.readInt()

    val fileStatistics = ColumnStatistics(in)

    new ColumnMeta(encoding, dictionaryDataLength, dictionaryIdSize, fileStatistics)
  }

  def unapply(columnMeta: ColumnMeta): Option[Array[Byte]] = {
    val buf = new ByteArrayOutputStream()
    val out = new DataOutputStream(buf)

    out.writeInt(columnMeta.encoding.getValue)
    out.writeInt(columnMeta.dictionaryDataLength)
    out.writeInt(columnMeta.dictionaryIdSize)

    columnMeta.fileStatistics match {
      case ColumnStatistics(bytes) => out.write(bytes)
    }

    Some(buf.toByteArray)
  }
}

private[oap] abstract class OapDataFileMeta extends DataFileMeta {
  var rowGroupsMeta: ArrayBuffer[RowGroupMeta]
  var columnsMeta: ArrayBuffer[ColumnMeta]
  var rowCountInEachGroup: Int
  var rowCountInLastGroup: Int
  var groupCount: Int
  var fieldCount: Int
}

private[oap] class OapDataFileMetaV1(
    override var rowGroupsMeta: ArrayBuffer[RowGroupMeta] = new ArrayBuffer[RowGroupMeta](),
    override var columnsMeta: ArrayBuffer[ColumnMeta] = new ArrayBuffer[ColumnMeta](),
    override var rowCountInEachGroup: Int = 0,
    override var rowCountInLastGroup: Int = 0,
    override var groupCount: Int = 0,
    override var fieldCount: Int = 0,
    var codec: CompressionCodec = CompressionCodec.UNCOMPRESSED) extends OapDataFileMeta {
  private var _fin: FSDataInputStream = _
  private var _len: Long = 0

  // Magic bytes and version number
  private val MAGIC_VERSION = "OAP1"

  def fin: FSDataInputStream = _fin
  def len: Long = _len

  def totalRowCount(): Int = {
    if (groupCount == 0) {
      0
    } else {
      (groupCount - 1) * rowCountInEachGroup + rowCountInLastGroup
    }
  }

  def appendRowGroupMeta(meta: RowGroupMeta): OapDataFileMetaV1 = {
    this.rowGroupsMeta.append(meta)
    this
  }

  def appendColumnMeta(meta: ColumnMeta): OapDataFileMetaV1 = {
    this.columnsMeta.append(meta)
    this
  }

  def withRowCountInLastGroup(count: Int): OapDataFileMetaV1 = {
    this.rowCountInLastGroup = count
    this
  }

  def withGroupCount(count: Int): OapDataFileMetaV1 = {
    this.groupCount = count
    this
  }

  private def validateConsistency(): Unit = {
    require(rowGroupsMeta.length == groupCount,
      s"Row Group Meta Count isn't equals to $groupCount")
    require(columnsMeta.length == fieldCount,
      s"Column Meta Count isn't equals to $fieldCount")
  }

  def write(os: FSDataOutputStream): Unit = {
    validateConsistency()

    val startPos = os.getPos
    os.writeBytes(MAGIC_VERSION)
    os.writeInt(this.rowCountInEachGroup)
    os.writeInt(this.rowCountInLastGroup)
    os.writeInt(this.groupCount)
    os.writeInt(this.fieldCount)
    os.writeInt(this.codec.getValue)

    columnsMeta.foreach { case ColumnMeta(bytes) => os.write(bytes) }

    rowGroupsMeta.foreach(_.write(os))
    val endPos = os.getPos
    // Write down the length of meta data
    os.writeInt((endPos - startPos).toInt)
  }

  def read(is: FSDataInputStream, fileLen: Long): OapDataFileMetaV1 = is.synchronized {
    this._fin = is
    this._len = fileLen

    val oapDataFileMetaLengthIndex = fileLen - 4

    // seek to the position of data file meta length
    is.seek(oapDataFileMetaLengthIndex)
    val oapDataFileMetaLength = is.readInt()

    // read all bytes of data file meta
    val metaBytes = new Array[Byte](oapDataFileMetaLength)

    is.readFully(oapDataFileMetaLengthIndex - oapDataFileMetaLength, metaBytes)

    val in = new DataInputStream(new ByteArrayInputStream(metaBytes))

    // Magic number and version has already been checked
    in.skipBytes(MAGIC_VERSION.length)

    this.rowCountInEachGroup = in.readInt()
    this.rowCountInLastGroup = in.readInt()
    this.groupCount = in.readInt()
    this.fieldCount = in.readInt()
    this.codec = CompressionCodec.findByValue(in.readInt())

    (0 until fieldCount).foreach(_ => columnsMeta.append(ColumnMeta(in)))

    (0 until groupCount).foreach(_ =>
      rowGroupsMeta.append(new RowGroupMeta().read(in, this.fieldCount)))

    validateConsistency()
    this
  }

  override def getGroupCount: Int = groupCount

  override def getFieldCount: Int = fieldCount
}
