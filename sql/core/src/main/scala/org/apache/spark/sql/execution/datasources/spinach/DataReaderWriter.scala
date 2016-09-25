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
      case (StructField(_, IntegerType, _, _), idx) => new IntDataReaderWriter(idx)
      case (StructField(_, StringType, _, _), idx) => new StringDataReaderWriter(idx)
      case (StructField(_, dt, _, _), idx) =>
        throw new UnsupportedOperationException(s"$idx:${dt.json}")
    })
}

private[spinach] class IntDataReaderWriter(cardinal: Int) extends DataReaderWriter(cardinal) {
  protected[this] def writeInternal(out: DataOutputStream, row: InternalRow): Unit = {
    out.writeInt(row.getInt(cardinal))
  }
  protected[this] def readInternal(in: DataInputStream, row: MutableRow): Unit = {
    row.setInt(cardinal, in.readInt())
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
