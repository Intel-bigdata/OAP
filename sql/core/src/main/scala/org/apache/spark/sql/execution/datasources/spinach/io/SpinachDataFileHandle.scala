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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.parquet.format.{CompressionCodec, Encoding}


//  Meta Part Format
//  ..
//  Field                               Length In Byte
//  Meta
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
//    Encoding                          4 * Field Count In Each Row
//      Column #1 Encoding              4
//      ...                             4
//      Column #N Encoding              4
//    Dictionary Bytes Length           4 * Field Count In Each Row
//      Column #1 Dict Bytes Length     4
//      ...                             4
//      Column #N Dict Bytes Length     4
//    Dictionary ID Size                4 * Field Count In Each Row
//      Column #1 Dict ID Size          4
//      ...                             4
//      Column #N Dict ID Size          4
//    Compression Codec                 4
//    Row Count In Each Row Group       4
//    Row Count In Last Row Group       4
//    Row Group Count                   4
//    Field Count In each Row           4

private[spinach] class RowGroupMeta {
  var start: Long = _
  var end: Long = _
  var fiberLens: Array[Int] = _
  var fiberUncompressedLens: Array[Int] = _

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

  def write(os: FSDataOutputStream): RowGroupMeta = {
    os.writeLong(start)
    os.writeLong(end)
    fiberLens.foreach(os.writeInt)
    fiberUncompressedLens.foreach(os.writeInt)

    this
  }

  def read(is: FSDataInputStream, fieldCount: Int): RowGroupMeta = {
    start = is.readLong()
    end = is.readLong()
    fiberLens = new Array[Int](fieldCount)
    fiberUncompressedLens = new Array[Int](fieldCount)

    (0 until fiberLens.length).foreach(fiberLens(_) = is.readInt())
    (0 until fiberUncompressedLens.length).foreach(fiberUncompressedLens(_) = is.readInt())

    this
  }
}

private[spinach] class SpinachDataFileHandle(
   var rowGroupsMeta: ArrayBuffer[RowGroupMeta] = new ArrayBuffer[RowGroupMeta](),
   var rowCountInEachGroup: Int = 0,
   var rowCountInLastGroup: Int = 0,
   var groupCount: Int = 0,
   var fieldCount: Int = 0,
   var codec: CompressionCodec = CompressionCodec.UNCOMPRESSED) extends DataFileHandle {
  private var _fin: FSDataInputStream = null
  private var _len: Long = 0

  var encodings: Array[Encoding] = _
  var dictionaryDataLens: Array[Int] = _
  var dictionaryIdSizes: Array[Int] = _

  def fin: FSDataInputStream = _fin
  def len: Long = _len

  def totalRowCount(): Int = {
      if (groupCount == 0) {
        0
      } else {
        (groupCount - 1) * rowCountInEachGroup + rowCountInLastGroup
      }
  }

  def appendRowGroupMeta(meta: RowGroupMeta): SpinachDataFileHandle = {
    this.rowGroupsMeta.append(meta)
    this
  }

  def withRowCountInLastGroup(count: Int): SpinachDataFileHandle = {
    this.rowCountInLastGroup = count
    this
  }

  def withGroupCount(count: Int): SpinachDataFileHandle = {
    this.groupCount = count
    this
  }

  def withEncodings(encodings: Array[Encoding]): SpinachDataFileHandle = {
    this.encodings = encodings
    this
  }

  def withDictionaryDataLens(dictionaryDataLens: Array[Int]): SpinachDataFileHandle = {
    this.dictionaryDataLens = dictionaryDataLens
    this
  }

  def withDictionaryIdSizes(dictionaryIdSizes: Array[Int]): SpinachDataFileHandle = {
    this.dictionaryIdSizes = dictionaryIdSizes
    this
  }

  private def validateConsistency(): Unit = {
    require(rowGroupsMeta.length == groupCount,
      s"Row Group Meta Count isn't equals to $groupCount")
  }

  def write(os: FSDataOutputStream): Unit = {
    validateConsistency()

    rowGroupsMeta.foreach(_.write(os))

    encodings.foreach(encoding => os.writeInt(encoding.getValue))
    dictionaryDataLens.foreach(os.writeInt)
    dictionaryIdSizes.foreach(os.writeInt)

    os.writeInt(this.codec.getValue)
    os.writeInt(this.rowCountInEachGroup)
    os.writeInt(this.rowCountInLastGroup)
    os.writeInt(this.groupCount)
    os.writeInt(this.fieldCount)
  }

  def read(is: FSDataInputStream, fileLen: Long): SpinachDataFileHandle = is.synchronized {
    this._fin = is
    this._len = fileLen

    // seek to the end of the end position of Meta
    is.seek(fileLen - 20L)
    this.codec = CompressionCodec.findByValue(is.readInt())
    this.rowCountInEachGroup = is.readInt()
    this.rowCountInLastGroup = is.readInt()
    this.groupCount = is.readInt()
    this.fieldCount = is.readInt()

    // seek to the start position of column meta
    is.seek(fileLen - 20L - fieldCount * 12)
    encodings = new Array[Encoding](fieldCount)
    dictionaryDataLens = new Array[Int](fieldCount)
    dictionaryIdSizes = new Array[Int](fieldCount)
    (0 until fieldCount).foreach(encodings(_) = Encoding.findByValue(is.readInt()))
    (0 until fieldCount).foreach(dictionaryDataLens(_) = is.readInt())
    (0 until fieldCount).foreach(dictionaryIdSizes(_) = is.readInt())

    // TODO: [Linhong] Lets' change this to some readable expression
    // seek to the start position of Meta
    val rowGroupMetaPos = fileLen - 20 - groupCount * (16 + 8 * fieldCount) - fieldCount * 12
    is.seek(rowGroupMetaPos)
    var i = 0
    while(i < groupCount) {
      rowGroupsMeta.append(new RowGroupMeta().read(is, this.fieldCount))
      i += 1
    }

    validateConsistency()
    this
  }
}
