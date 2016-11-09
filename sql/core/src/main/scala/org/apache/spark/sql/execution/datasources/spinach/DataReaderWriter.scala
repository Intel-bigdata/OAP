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

package org.apache.spark.sql.execution.datasources.spinach

import java.io.{DataInputStream, DataOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[spinach] abstract class DataReaderWriter(val cardinal: Int) {
  def write(out: DataOutputStream, row: InternalRow): Unit = {
    if (row.isNullAt(cardinal)) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      writeInternal(out, row)
    }
  }

  def read(in: DataInputStream, row: MutableRow): Unit = {
    if (in.readBoolean()) {
      readInternal(in, row)
    } else {
      row.setNullAt(cardinal)
    }
  }

  protected[this] def writeInternal(out: DataOutputStream, row: InternalRow)
  protected[this] def readInternal(in: DataInputStream, obj: MutableRow)
}

private[spinach] object DataReaderWriter {
  def initialDataReaderWriterFromSchema(schema: StructType): Array[DataReaderWriter] =
    schema.fields.zipWithIndex.map(_ match {
      case (StructField(_, StringType, _, _), idx) => new StringDataReaderWriter(idx)
      case (StructField(_, dt, _, _), idx) if dt.defaultSize <= 8 =>
        new FixedSizeDataReaderWriter(idx, dt)
      case (StructField(_, dt, _, _), idx) =>
        throw new UnsupportedOperationException(s"$idx:${dt.json}")
    })
}

private[spinach] class FixedSizeDataReaderWriter(ordinal: Int, dataType: DataType)
  extends DataReaderWriter(ordinal) {
  protected[this] def writeInternal(out: DataOutputStream, row: InternalRow): Unit = {
    dataType match {
      case BooleanType =>
        out.writeBoolean(row.getBoolean(ordinal))
      case ByteType =>
        out.writeByte(row.getByte(ordinal))
      case DateType =>
        out.writeInt(row.getInt(ordinal))
      case DoubleType =>
        out.writeDouble(row.getDouble(ordinal))
      case FloatType =>
        out.writeFloat(row.getFloat(ordinal))
      case IntegerType =>
        out.writeInt (row.getInt (ordinal) )
      case LongType =>
        out.writeLong(row.getLong(ordinal))
      case ShortType =>
        out.writeShort(row.getShort(ordinal))
      case TimestampType =>
        out.writeLong(row.getLong(ordinal))
      case _ =>
        throw new NotImplementedError("TODO")
    }
  }
  protected[this] def readInternal(in: DataInputStream, row: MutableRow): Unit = {
    dataType match {
      case BooleanType =>
        row.setBoolean(ordinal, in.readBoolean())
      case ByteType =>
        row.setByte(ordinal, in.readByte())
      case DateType =>
        row.setInt(ordinal, in.readInt())
      case DoubleType =>
        row.setDouble(ordinal, in.readDouble())
      case FloatType =>
        row.setFloat(ordinal, in.readFloat())
      case IntegerType =>
        row.setInt(ordinal, in.readInt())
      case LongType =>
        row.setLong(ordinal, in.readLong())
      case ShortType =>
        row.setShort(ordinal, in.readShort())
      case TimestampType =>
        row.setLong(ordinal, in.readLong())
      case _ =>
        throw new NotImplementedError("TODO")
    }
  }
}

private[spinach] class StringDataReaderWriter(cardinal: Int) extends DataReaderWriter(cardinal) {
  protected[this] def writeInternal(out: DataOutputStream, row: InternalRow): Unit = {
    val s = row.getUTF8String(cardinal)
    val len = s.numBytes()
    out.writeInt(len)
    out.write(s.getBytes)
  }

  protected[this] def readInternal(in: DataInputStream, row: MutableRow): Unit = {
    val len = in.readInt()
    val bytes = new Array[Byte](len)
    in.readFully(bytes)
    row.update(cardinal, UTF8String.fromBytes(bytes))
  }
}
