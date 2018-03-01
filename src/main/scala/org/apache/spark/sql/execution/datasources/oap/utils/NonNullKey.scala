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

package org.apache.spark.sql.execution.datasources.oap.utils

import java.io.OutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index.IndexUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Initialize only once for every task for a static schema, to avoid pattern matching and
 * branching when directly use `writeBasedOnSchema` and `writeBasedOnDataType`.
 */
private[oap] class NonNullKeyWriter(schema: StructType) {
  private lazy val internalWriters: Seq[(OutputStream, Any) => Unit] =
    schema.toSeq.map(_.dataType).map(getWriterFunctionBasedOnType)

  private def getWriterFunctionBasedOnType(
      dt: DataType): (OutputStream, Any) => Unit = dt match {
    case BooleanType =>
      (out: OutputStream, b: Any) => IndexUtils.writeBoolean(out, b.asInstanceOf[Boolean])
    case ByteType =>
      (out: OutputStream, b: Any) => IndexUtils.writeByte(out, b.asInstanceOf[Byte])
    case ShortType =>
      (out: OutputStream, sh: Any) => IndexUtils.writeShort(out, sh.asInstanceOf[Short])
    case IntegerType =>
      (out: OutputStream, i: Any) => IndexUtils.writeInt(out, i.asInstanceOf[Int])
    case LongType =>
      (out: OutputStream, l: Any) => IndexUtils.writeLong(out, l.asInstanceOf[Long])
    case FloatType =>
      (out: OutputStream, f: Any) => IndexUtils.writeFloat(out, f.asInstanceOf[Float])
    case DoubleType =>
      (out: OutputStream, d: Any) => IndexUtils.writeDouble(out, d.asInstanceOf[Double])
    case StringType =>
      (out: OutputStream, s: Any) =>
        val bytes = s.asInstanceOf[UTF8String].getBytes
        IndexUtils.writeInt(out, bytes.length)
        out.write(bytes)
    case BinaryType =>
      (out: OutputStream, b: Any) =>
        val binary = b.asInstanceOf[Array[Byte]]
        IndexUtils.writeInt(out, binary.length)
        out.write(binary)
    case other => throw new OapException(s"OAP index currently doesn't support data type $other")
  }

  def writeKey(out: OutputStream, key: InternalRow): Unit = {
    internalWriters.zipWithIndex.foreach { wi =>
      val idx = wi._2
      val writer: (OutputStream, Any) => Unit = wi._1
      val value = key.get(idx, schema(idx).dataType)
      writer(out, value)
    }
  }
}

private[oap] class NonNullKeyReader(schema: StructType) {
  private lazy val internalReaders: Seq[(FiberCache, Int) => (Any, Int)] =
    schema.toSeq.map(_.dataType).map(getReaderFunctionBasedOnType)
  private def getReaderFunctionBasedOnType(dt: DataType): (FiberCache, Int) => (Any, Int) =
    (fiberCache: FiberCache, offset: Int) => dt match {
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
        val string = fiberCache.getUTF8String(offset + IndexUtils.INT_SIZE, length)
        (string, IndexUtils.INT_SIZE + length)
      case BinaryType =>
        val length = fiberCache.getInt(offset)
        val bytes = fiberCache.getBytes(offset + IndexUtils.INT_SIZE, length)
        (bytes, IndexUtils.INT_SIZE + bytes.length)
      case other => throw new OapException(s"OAP index currently doesn't support data type $other")
    }

  def readKey(fiberCache: FiberCache, offset: Int): (InternalRow, Int) = {
    var pos = offset
    val values = internalReaders.map { reader =>
      val (value, length) = reader(fiberCache, pos)
      pos += length
      value
    }
    (InternalRow.fromSeq(values), pos - offset)
  }
}
