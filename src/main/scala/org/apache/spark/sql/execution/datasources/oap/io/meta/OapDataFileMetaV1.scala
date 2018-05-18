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

package org.apache.spark.sql.execution.datasources.oap.io.meta

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.parquet.format.{CompressionCodec, Encoding}

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.unsafe.types.UTF8String

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

private[oap] class OapDataFileMetaV1(
    var rowGroupsMeta: ArrayBuffer[RowGroupMetaV1] = new ArrayBuffer[RowGroupMetaV1](),
    var columnsMeta: ArrayBuffer[ColumnMetaV1] = new ArrayBuffer[ColumnMetaV1](),
    var rowCountInEachGroup: Int = 0,
    var rowCountInLastGroup: Int = 0,
    var groupCount: Int = 0,
    var fieldCount: Int = 0,
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

  def appendRowGroupMeta(meta: RowGroupMetaV1): OapDataFileMetaV1 = {
    this.rowGroupsMeta.append(meta)
    this
  }

  def appendColumnMeta(meta: ColumnMetaV1): OapDataFileMetaV1 = {
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

    columnsMeta.foreach { case ColumnMetaV1(bytes) => os.write(bytes) }

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

    val buffer = new Array[Byte](MAGIC_VERSION.length)
    in.readFully(buffer)
    val magic = UTF8String.fromBytes(buffer).toString
    if (magic != MAGIC_VERSION) {
      throw new OapException("Not a valid Oap Data File")
    }

    this.rowCountInEachGroup = in.readInt()
    this.rowCountInLastGroup = in.readInt()
    this.groupCount = in.readInt()
    this.fieldCount = in.readInt()
    this.codec = CompressionCodec.findByValue(in.readInt())

    (0 until fieldCount).foreach(_ => columnsMeta.append(ColumnMetaV1(in)))

    (0 until groupCount).foreach(_ =>
      rowGroupsMeta.append(new RowGroupMetaV1().read(in, this.fieldCount)))

    validateConsistency()
    this
  }

  override def getGroupCount: Int = groupCount

  override def getFieldCount: Int = fieldCount
}

private[oap] class RowGroupMetaV1 {
  var start: Long = _
  var end: Long = _
  var fiberLens: Array[Int] = _
  var fiberUncompressedLens: Array[Int] = _
  var statistics: Array[ColumnStatistics] = _

  def withNewStart(newStart: Long): RowGroupMetaV1 = {
    this.start = newStart
    this
  }

  def withNewEnd(newEnd: Long): RowGroupMetaV1 = {
    this.end = newEnd
    this
  }

  def withNewFiberLens(newFiberLens: Array[Int]): RowGroupMetaV1 = {
    this.fiberLens = newFiberLens
    this
  }

  def withNewUncompressedFiberLens(newUncompressedFiberLens: Array[Int]): RowGroupMetaV1 = {
    this.fiberUncompressedLens = newUncompressedFiberLens
    this
  }

  def withNewStatistics(newStatistics: Array[ColumnStatistics]): RowGroupMetaV1 = {
    this.statistics = newStatistics
    this
  }

  def write(os: FSDataOutputStream): RowGroupMetaV1 = {
    os.writeLong(start)
    os.writeLong(end)
    fiberLens.foreach(os.writeInt)
    fiberUncompressedLens.foreach(os.writeInt)
    statistics.foreach {
      case ColumnStatistics(bytes) => os.write(bytes)
    }
    this
  }

  def read(is: DataInputStream, fieldCount: Int): RowGroupMetaV1 = {
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

private[oap] class ColumnMetaV1(
    val encoding: Encoding,
    val dictionaryDataLength: Int,
    val dictionaryIdSize: Int,
    val fileStatistics: ColumnStatistics) {}

private[oap] object ColumnMetaV1 {

  def apply(in: DataInputStream): ColumnMetaV1 = {

    val encoding = Encoding.findByValue(in.readInt())
    val dictionaryDataLength = in.readInt()
    val dictionaryIdSize = in.readInt()

    val fileStatistics = ColumnStatistics(in)

    new ColumnMetaV1(encoding, dictionaryDataLength, dictionaryIdSize, fileStatistics)
  }

  def unapply(columnMeta: ColumnMetaV1): Option[Array[Byte]] = {
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
