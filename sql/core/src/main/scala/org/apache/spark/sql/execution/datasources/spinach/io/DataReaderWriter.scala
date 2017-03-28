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

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


private[spinach] abstract class DataReaderWriter(val cardinal: Int) {
  def write(out: FSDataOutputStream, row: InternalRow): Unit = {
    if (row.isNullAt(cardinal)) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      writeInternal(out, row)
    }
  }

  def read(in: FSDataInputStream, row: MutableRow): Unit = {
    if (in.readBoolean()) {
      readInternal(in, row)
    } else {
      row.setNullAt(cardinal)
    }
  }

  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow)
  protected[this] def readInternal(in: FSDataInputStream, obj: MutableRow)
}

private[spinach] object DataReaderWriter {
  def initialDataReaderWriterFromSchema(schema: StructType): Array[DataReaderWriter] =
    schema.fields.zipWithIndex.map(_ match {
      case (StructField(_, BooleanType, _, _), idx) => new BoolDataReaderWriter(idx)
      case (StructField(_, ByteType, _, _), idx) => new ByteDataReaderWriter(idx)
      case (StructField(_, ShortType, _, _), idx) => new ShortDataReaderWriter(idx)
      case (StructField(_, IntegerType, _, _), idx) => new IntDataReaderWriter(idx)
      case (StructField(_, LongType, _, _), idx) => new LongDataReaderWriter(idx)
      case (StructField(_, FloatType, _, _), idx) => new FloatDataReaderWriter(idx)
      case (StructField(_, DoubleType, _, _), idx) => new DoubleDataReaderWriter(idx)
      case (StructField(_, DateType, _, _), idx) => new DateDataReaderWriter(idx)
      case (StructField(_, TimestampType, _, _), idx) => new TimestampDataReaderWriter(idx)
      case (StructField(_, StringType, _, _), idx) => new StringDataReaderWriter(idx)
      case (StructField(_, dt, _, _), idx) =>
        throw new UnsupportedOperationException(s"$idx:${dt.json}")
    })
}

private[spinach] class BoolDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeBoolean(row.getBoolean(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setBoolean(ordinal, in.readBoolean())
  }
}

private[spinach] class ByteDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeByte(row.getByte(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setByte(ordinal, in.readByte())
  }
}

private[spinach] class ShortDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeShort(row.getShort(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setShort(ordinal, in.readShort())
  }
}

private[spinach] class IntDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeInt(row.getInt(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setInt(ordinal, in.readInt())
  }
}

private[spinach] class LongDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeLong(row.getLong(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setLong(ordinal, in.readLong())
  }
}

private[spinach] class FloatDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeFloat(row.getFloat(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setFloat(ordinal, in.readFloat())
  }
}

private[spinach] class DoubleDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeDouble(row.getDouble(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setDouble(ordinal, in.readDouble())
  }
}

private[spinach] class DateDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeInt(row.getInt(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setInt(ordinal, in.readInt())
  }
}

private[spinach] class TimestampDataReaderWriter(ordinal: Int) extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    out.writeLong(row.getLong(ordinal))
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    row.setLong(ordinal, in.readLong())
  }
}

private[spinach] class StringDataReaderWriter(cardinal: Int) extends DataReaderWriter(cardinal) {
  protected[this] def writeInternal(out: FSDataOutputStream, row: InternalRow): Unit = {
    val s = row.getUTF8String(cardinal)
    val len = s.numBytes()
    out.writeInt(len)
    out.write(s.getBytes)
  }

  protected[this] def readInternal(in: FSDataInputStream, row: MutableRow): Unit = {
    val len = in.readInt()
    val bytes = new Array[Byte](len)
    in.readFully(bytes)
    row.update(cardinal, UTF8String.fromBytes(bytes))
  }
}
