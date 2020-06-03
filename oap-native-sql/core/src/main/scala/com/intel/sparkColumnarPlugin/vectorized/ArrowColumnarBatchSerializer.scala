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

package com.intel.sparkColumnarPlugin.vectorized

import java.io._
import java.nio.ByteBuffer

import com.intel.sparkColumnarPlugin.expression.CodeGeneration
import com.intel.sparkColumnarPlugin.vectorized.ArrowWritableColumnVector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.util.ArrowUtils

import org.apache.spark.serializer.{
  DeserializationStream,
  SerializationStream,
  Serializer,
  SerializerInstance
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.reflect.ClassTag

class ArrowColumnarBatchSerializer extends Serializer with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance =
    new ArrowColumnarBatchSerializerInstance
}

private class ArrowColumnarBatchSerializerInstance extends SerializerInstance {

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val root = createVectorSchemaRoot(value.asInstanceOf[ColumnarBatch])
      val writer = new ArrowStreamWriter(root, null, out)
      writer.writeBatch()
      writer.end()
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      out.flush()
    }

    override def close(): Unit = {
      out.close()
    }

    private def createVectorSchemaRoot(cb: ColumnarBatch): VectorSchemaRoot = {
      val fieldTypesList = List
        .range(0, cb.numCols())
        .map(i => Field.nullable(s"c_$i", CodeGeneration.getResultType(cb.column(i).dataType())))
      val arrowSchema = new Schema(fieldTypesList.asJava)
      val vectors = List
        .range(0, cb.numCols())
        .map(
          i =>
            cb.column(i)
              .asInstanceOf[ArrowWritableColumnVector]
              .getValueVector
              .asInstanceOf[FieldVector])
      val root = new VectorSchemaRoot(arrowSchema, vectors.asJava, cb.numRows)
      root.setRowCount(cb.numRows)
      root
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {

      private[this] val columnBatchSize = SQLConf.get.columnBatchSize
      private[this] var allocator: BufferAllocator = _

      private[this] var reader: ArrowStreamReader = _
      private[this] var root: VectorSchemaRoot = _
      private[this] var vectors: Array[ArrowWritableColumnVector] = _

      // TODO: see if asKeyValueIterator should be override

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      @throws(classOf[EOFException])
      override def readValue[T: ClassTag](): T = {
        try {
          allocator = ArrowUtils.rootAllocator
            .newChildAllocator("ArrowColumnarBatch deserialize", 0, Long.MaxValue)
          reader = new ArrowStreamReader(in, allocator)
          root = reader.getVectorSchemaRoot
          //          vectors = new ArrayBuffer[ColumnVector]()
        } catch {
          case _: IOException =>
            reader.close(false)
            root.close()
            throw new EOFException
        }

        var numRows = 0
        // "root.rowCount" is set to 0 when reader.loadNextBatch reach EOF
        while (reader.loadNextBatch()) {
          numRows += root.getRowCount
        }

        assert(
          numRows <= columnBatchSize,
          "the number of loaded rows exceed the maximum columnar batch size")

        vectors = ArrowWritableColumnVector.loadColumns(numRows, root.getFieldVectors)
        val cb = new ColumnarBatch(vectors.toArray, numRows)
        // the underlying ColumnVectors of this ColumnarBatch might be empty
        cb.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (reader != null) reader.close(false)
        if (root != null) root.close()
        in.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
