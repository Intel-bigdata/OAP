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
import org.apache.parquet.format.Encoding


//  Meta Part Format
//  ..
//  Field                               Length In Byte
//  Meta
//    RowGroup Meta #1                  16 + 4 * Field Count In Each Row * 2
//      RowGroup StartPosition          8
//      RowGroup EndPosition            8
//      Fiber #1 Length                 4
//      Fiber #2 Length                 4
//      ...
//      Fiber #N Length                 4
//      Fiber #1 Uncompressed Length    4
//      Fiber #2 Uncompressed Length    4
//      ...
//      Fiber #N Uncompressed Length    4
//    RowGroup Meta #2                  16 + 4 * Field Count In Each Row * 2
//    RowGroup Meta #3                  16 + 4 * Field Count In Each Row * 2
//    ..                                16 + 4 * Field Count In Each Row * 2
//    RowGroup Meta #N                  16 + 4 * Field Count In Each Row * 2
//    Column Encoding                   4 * Field Count In Each Row
//      Column #1 Encoding              4
//      Column #2 Encoding              4
//      ...
//      Column #N Encoding              4
//    Column Dict Data Length           4 * Field Count In Each Row
//      Column #1 Dict Data Length      4
//      Column #2 Dict Data Length      4
//      ...
//      Column #N Dict Data Length      4
//    Column Dict Size                  4 * Field Count In Each Row
//      Column #1 Dict Size             4
//      Column #2 Dict Size             4
//      ...
//      Column #N Dict Size             4
//    Compression Codec                 4
//    Row Count In Each Row Group       4
//    Row Count In Last Row Group       4
//    Row Group Count                   4
//    Field Count In each Row           4

private[spinach] class RowGroupMeta {

  var start: Long = _
  var end: Long = _
  var fiberLens: Array[Int] = _
  var fiberCompressedLens: Array[Int] = _
  var fiberEncodings: Array[Encoding] = _

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

  def withNewFiberCompressedLens(fiberCompressedLens: Array[Int]): RowGroupMeta = {
    this.fiberCompressedLens = fiberCompressedLens
    this
  }

  def withNewFiberEncodings(newFiberEncodings: Array[Encoding]): RowGroupMeta = {
    this.fiberEncodings = newFiberEncodings
    this
  }

  def write(os: FSDataOutputStream): RowGroupMeta = {
    os.writeLong(start)
    os.writeLong(end)
    var i = 0
    while (i < fiberLens.length) {
      os.writeInt(fiberLens(i))
      i += 1
    }

    i = 0
    while (i < fiberCompressedLens.length) {
      os.writeInt(fiberCompressedLens(i))
      i += 1
    }

    i = 0
    while (i < fiberEncodings.length) {
      os.writeInt(fiberEncodings(i).getValue)
      i += 1
    }

    this
  }

  def read(is: FSDataInputStream, fieldCount: Int): RowGroupMeta = {
    start = is.readLong()
    end = is.readLong()
    fiberLens = new Array[Int](fieldCount)
    fiberCompressedLens = new Array[Int](fieldCount)
    fiberEncodings = new Array[Encoding](fieldCount)

    var idx = 0
    while(idx < fieldCount) {
      fiberLens(idx) = is.readInt()
      idx += 1
    }

    idx = 0
    while(idx < fieldCount) {
      fiberCompressedLens(idx) = is.readInt()
      idx += 1
    }

    idx = 0
    while (idx < fieldCount) {
      fiberEncodings(idx) = Encoding.findByValue(is.readInt())
      idx += 1
    }

    this
  }
}

private[spinach] class SpinachDataFileHandle(
   var rowGroupsMeta: ArrayBuffer[RowGroupMeta] = new ArrayBuffer[RowGroupMeta](),
   var rowCountInEachGroup: Int = 0,
   var rowCountInLastGroup: Int = 0,
   var groupCount: Int = 0,
   var fieldCount: Int = 0) extends DataFileHandle {

  var dictDataLens: Array[Int] = _
  var dictSizes: Array[Int] = _

  private var _fin: FSDataInputStream = null
  private var _len: Long = 0

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

  def withDictDataLens(dictDataLengths: Array[Int]): SpinachDataFileHandle = {
    this.dictDataLens = dictDataLengths
    this
  }

  def withDictSizes(dictSizes: Array[Int]): SpinachDataFileHandle = {
    this.dictSizes = dictSizes
    this
  }

  private def validateConsistency(): Unit = {
    require(rowGroupsMeta.length == groupCount,
      s"Row Group Meta Count isn't equals to $groupCount")
  }

  def write(os: FSDataOutputStream): Unit = {
    validateConsistency()

    var i = 0
    while (i < this.groupCount) {
      rowGroupsMeta(i).write(os)
      i += 1
    }

    i = 0
    while (i < fieldCount) {
      os.writeInt(dictDataLens(i))
      i += 1
    }

    i = 0
    while (i < fieldCount) {
      os.writeInt(dictSizes(i))
      i += 1
    }

    os.writeInt(this.rowCountInEachGroup)
    os.writeInt(this.rowCountInLastGroup)
    os.writeInt(this.groupCount)
    os.writeInt(this.fieldCount)
  }

  def read(is: FSDataInputStream, fileLen: Long): SpinachDataFileHandle = is.synchronized {
    this._fin = is
    this._len = fileLen

    // seek to the end of the end position of Meta
    is.seek(fileLen - 16L)
    this.rowCountInEachGroup = is.readInt()
    this.rowCountInLastGroup = is.readInt()
    this.groupCount = is.readInt()
    this.fieldCount = is.readInt()

    is.seek(fileLen - 16L - this.fieldCount * 8)
    this.dictDataLens = new Array[Int](fieldCount)
    this.dictSizes = new Array[Int](fieldCount)
    var i = 0
    while (i < fieldCount) {
      this.dictDataLens(i) = is.readInt()
      i += 1
    }
    i = 0
    while (i < fieldCount) {
      this.dictSizes(i) = is.readInt()
      i += 1
    }
    // seek to the start position of Meta
    val rowGroupMetaPos = fileLen - 16  - this.fieldCount * 8 - groupCount * (16 + 4 * fieldCount * 3)
    is.seek(rowGroupMetaPos)
    i = 0
    while(i < groupCount) {
      rowGroupsMeta.append(new RowGroupMeta().read(is, this.fieldCount))
      i += 1
    }

    validateConsistency()
    this
  }
}
