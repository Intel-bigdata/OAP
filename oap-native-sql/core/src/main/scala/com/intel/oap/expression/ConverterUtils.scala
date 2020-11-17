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

package com.intel.oap.expression

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.nio.channels.Channels
import java.nio.ByteBuffer
import java.util.ArrayList
import com.intel.oap.vectorized.ArrowWritableColumnVector
import io.netty.buffer.{ArrowBuf, ByteBufAllocator, ByteBufOutputStream}
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression.ExpressionTree
import org.apache.arrow.gandiva.ipc.GandivaTypes
import org.apache.arrow.gandiva.ipc.GandivaTypes.ExpressionList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{
  ArrowFieldNode,
  ArrowRecordBatch,
  IpcOption,
  MessageResult,
  MessageSerializer,
  MessageChannelReader
}
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import io.netty.buffer.{ByteBuf, ByteBufAllocator, ByteBufOutputStream}
import java.nio.channels.{Channels, WritableByteChannel}
import com.google.common.collect.Lists
import java.io.{InputStream, OutputStream}

object ConverterUtils extends Logging {
  def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val numRowsInBatch = columnarBatch.numRows()
    val cols = (0 until columnarBatch.numCols).toList.map(i =>
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector())
    createArrowRecordBatch(numRowsInBatch, cols)

    /*val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    for (i <- 0 until columnarBatch.numCols()) {
      val inputVector =
        columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      //FIXME for projection + in test
      //fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      if (inputVector.isInstanceOf[VarCharVector]) {
        inputData += inputVector.getOffsetBuffer()
      }
      inputData += inputVector.getDataBuffer()
      //FIXME for projection + in test
      //inputData += inputVector.getValidityBuffer()
    }
    new ArrowRecordBatch(numRowsInBatch, fieldNodes.toList.asJava, inputData.toList.asJava)*/
  }

  def createArrowRecordBatch(numRowsInBatch: Int, cols: List[ValueVector]): ArrowRecordBatch = {
    val nodes = new java.util.ArrayList[ArrowFieldNode]()
    val buffers = new java.util.ArrayList[ArrowBuf]()
    cols.foreach(vector => {
      appendNodes(vector.asInstanceOf[FieldVector], nodes, buffers);
    })
    new ArrowRecordBatch(numRowsInBatch, nodes, buffers);
  }

  def appendNodes(
      vector: FieldVector,
      nodes: java.util.List[ArrowFieldNode],
      buffers: java.util.List[ArrowBuf]): Unit = {
    nodes.add(new ArrowFieldNode(vector.getValueCount, vector.getNullCount))
    val fieldBuffers = vector.getFieldBuffers
    val expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType())
    if (fieldBuffers.size != expectedBufferCount) {
      throw new IllegalArgumentException(
        s"wrong number of buffers for field ${vector.getField} in vector ${vector.getClass.getSimpleName}. found: ${fieldBuffers}")
    }
    buffers.addAll(fieldBuffers)
    vector.getChildrenFromFields.asScala.foreach(child => appendNodes(child, nodes, buffers))
  }

  def convertToNetty(iter: Array[ColumnarBatch], out: OutputStream): Unit = {
    val channel = new WriteChannel(Channels.newChannel(out))
    var schema: Schema = null
    val option = new IpcOption

    iter.foreach { columnarBatch =>
      val vectors = (0 until columnarBatch.numCols)
        .map(i => columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector])
        .toList
      if (schema == null) {
        schema = new Schema(vectors.map(_.getValueVector().getField).asJava)
        MessageSerializer.serialize(channel, schema, option)
      }
      MessageSerializer.serialize(
        channel,
        ConverterUtils
          .createArrowRecordBatch(columnarBatch.numRows, vectors.map(_.getValueVector)),
        option)
    }
  }

  def convertToNetty(iter: Array[ColumnarBatch]): Array[Byte] = {
    val innerBuf = ByteBufAllocator.DEFAULT.buffer()
    val outStream = new ByteBufOutputStream(innerBuf)
    convertToNetty(iter, outStream)
    val bytes = new Array[Byte](innerBuf.readableBytes);
    innerBuf.getBytes(innerBuf.readerIndex, bytes);
    innerBuf.release()
    outStream.close()
    bytes
  }

  def convertFromNetty(
      attributes: Seq[Attribute],
      input: InputStream): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      val allocator = ArrowWritableColumnVector.getOffRecordAllocator
      var messageReader =
        new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator)
      var schema: Schema = null
      var result: MessageResult = null

      override def hasNext: Boolean =
        if (input.available > 0) {
          return true
        } else {
          messageReader.close
          return false
        }
      override def next(): ColumnarBatch = {
        if (input.available == 0) {
          if (attributes == null) {
            return null
          }
          val resultStructType = StructType(
            attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
          return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
        }
        try {
          if (schema == null) {
            result = messageReader.readNext();
            if (result == null) {
              throw new IOException("Unexpected end of input. Missing schema.");
            }
            if (result.getMessage().headerType() != MessageHeader.Schema) {
              throw new IOException(
                "Expected schema but header was " + result.getMessage().headerType());
            }
            schema = MessageSerializer.deserializeSchema(result.getMessage());
          }
          result = messageReader.readNext();
          if (result.getMessage().headerType() != MessageHeader.RecordBatch) {
            throw new IOException(
              "Expected recordbatch but header was " + result.getMessage().headerType());
          }
          var bodyBuffer = result.getBodyBuffer();

          // For zero-length batches, need an empty buffer to deserialize the batch
          if (bodyBuffer == null) {
            bodyBuffer = allocator.getEmpty();
          }

          val batch = MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
          val vectors = fromArrowRecordBatch(schema, batch, allocator)
          val length = batch.getLength
          batch.close
          new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), length)
        } catch {
          case e: Throwable =>
            messageReader.close
            throw e
        }
      }
    }
  }

  def convertFromNetty(
      attributes: Seq[Attribute],
      data: Array[Array[Byte]]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var array_id = 0
      val allocator = ArrowWritableColumnVector.getOffRecordAllocator
      var input = new ByteArrayInputStream(data(array_id))
      var messageReader =
        new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator)
      var schema: Schema = null
      var result: MessageResult = null

      override def hasNext: Boolean =
        if (array_id < (data.size - 1) || input.available > 0) {
          return true
        } else {
          messageReader.close
          return false
        }
      override def next(): ColumnarBatch = {
        if (input.available == 0) {
          messageReader.close
          array_id += 1
          input = new ByteArrayInputStream(data(array_id))
          messageReader =
            new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator)
        }
        if (input.available == 0) {
          val resultStructType = StructType(
            attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
          return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
        }
        try {
          if (schema == null) {
            result = messageReader.readNext();

            if (result == null) {
              throw new IOException("Unexpected end of input. Missing schema.");
            }

            if (result.getMessage().headerType() != MessageHeader.Schema) {
              throw new IOException(
                "Expected schema but header was " + result.getMessage().headerType());
            }

            schema = MessageSerializer.deserializeSchema(result.getMessage());

          }

          result = messageReader.readNext();
          if (result.getMessage().headerType() == MessageHeader.Schema) {
            result = messageReader.readNext();
          }

          if (result.getMessage().headerType() != MessageHeader.RecordBatch) {
            throw new IOException(
              "Expected recordbatch but header was " + result.getMessage().headerType());
          }
          var bodyBuffer = result.getBodyBuffer();

          // For zero-length batches, need an empty buffer to deserialize the batch
          if (bodyBuffer == null) {
            bodyBuffer = allocator.getEmpty();
          }

          val batch = MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
          val vectors = fromArrowRecordBatch(schema, batch, allocator)
          val length = batch.getLength
          batch.close
          new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), length)
        } catch {
          case e: Throwable =>
            messageReader.close
            throw e
        }
      }
    }
  }

  def fromArrowRecordBatch(
      recordBatchSchema: Schema,
      recordBatch: ArrowRecordBatch,
      allocator: BufferAllocator = null): Array[ArrowWritableColumnVector] = {
    val numRows = recordBatch.getLength()
    ArrowWritableColumnVector.loadColumns(numRows, recordBatchSchema, recordBatch, allocator)
  }

  def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    if (recordBatch != null) {
      recordBatch.close()
    }
  }

  def releaseArrowRecordBatchList(recordBatchList: Array[ArrowRecordBatch]): Unit = {
    recordBatchList.foreach({ recordBatch =>
      if (recordBatch != null)
        releaseArrowRecordBatch(recordBatch)
    })
  }

  def getAttrFromExpr(fieldExpr: Expression, skipAlias: Boolean = false): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        getAttrFromExpr(a.child)
      case a: AggregateExpression =>
        getAttrFromExpr(a.aggregateFunction.children(0))
      case a: AttributeReference =>
        a
      case a: Alias =>
        if (skipAlias && a.child.isInstanceOf[AttributeReference])
          getAttrFromExpr(a.child)
        else
          a.toAttribute.asInstanceOf[AttributeReference]
      case a: KnownFloatingPointNormalized =>
        logInfo(s"$a")
        getAttrFromExpr(a.child)
      case a: NormalizeNaNAndZero =>
        getAttrFromExpr(a.child)
      case c: Coalesce =>
        getAttrFromExpr(c.children(0))
      case i: IsNull =>
        getAttrFromExpr(i.child)
      case a: Add =>
        getAttrFromExpr(a.left)
      case s: Subtract =>
        getAttrFromExpr(s.left)
      case u: Upper =>
        getAttrFromExpr(u.child)
      case ss: Substring =>
        getAttrFromExpr(ss.children(0))
      case other =>
        throw new UnsupportedOperationException(
          s"makeStructField is unable to parse from $other (${other.getClass}).")
    }
  }

  def getResultAttrFromExpr(
      fieldExpr: Expression,
      name: String = "None",
      dataType: Option[DataType] = None): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        val c = getResultAttrFromExpr(a.child, name, Some(a.dataType))
        AttributeReference(c.name, a.dataType, c.nullable, c.metadata)(c.exprId, c.qualifier)
      case a: AttributeReference =>
        if (name != "None") {
          new AttributeReference(name, a.dataType, a.nullable)()
        } else {
          a
        }
      case a: Alias =>
        if (name != "None") {
          a.toAttribute.asInstanceOf[AttributeReference].withName(name)
        } else {
          a.toAttribute.asInstanceOf[AttributeReference]
        }
      case d: ColumnarDivide =>
        new AttributeReference(name, DoubleType, d.nullable)()
      case m: ColumnarMultiply =>
        new AttributeReference(name, m.dataType, m.nullable)()
      case other =>
        val a = if (name != "None") {
          new Alias(other, name)()
        } else {
          new Alias(other, "res")()
        }
        val tmpAttr = a.toAttribute.asInstanceOf[AttributeReference]
        if (dataType.isDefined) {
          new AttributeReference(tmpAttr.name, dataType.getOrElse(null), tmpAttr.nullable)()
        } else {
          tmpAttr
        }
    }
  }

  def getColumnarFuncNode(
      expr: Expression,
      attributes: Seq[Attribute] = null): (TreeNode, ArrowType) = {
    if (expr.isInstanceOf[AttributeReference] && expr
          .asInstanceOf[AttributeReference]
          .name == "none") {
      throw new UnsupportedOperationException(
        s"Unsupport to generate native expression from replaceable expression.")
    }
    var columnarExpr: Expression = if (attributes != null) {
      ColumnarExpressionConverter.replaceWithColumnarExpression(
        expr,
        attributeSeq = attributes,
        convertBoundRefToAttrRef = true)
    } else {
      ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
    }
    var inputList: java.util.List[Field] = Lists.newArrayList()
    columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(inputList)
  }

  def ifEquals(left: Seq[AttributeReference], right: Seq[NamedExpression]): Boolean = {
    if (left.size != right.size) return false
    for (i <- 0 until left.size) {
      if (left(i).exprId != right(i).exprId) return false
    }
    true
  }

  def combineArrowRecordBatch(rb1: ArrowRecordBatch, rb2: ArrowRecordBatch): ArrowRecordBatch = {
    val numRows = rb1.getLength()
    val rb1_nodes = rb1.getNodes()
    val rb2_nodes = rb2.getNodes()
    val rb1_bufferlist = rb1.getBuffers()
    val rb2_bufferlist = rb2.getBuffers()

    val combined_nodes = rb1_nodes.addAll(rb2_nodes)
    val combined_bufferlist = rb1_bufferlist.addAll(rb2_bufferlist)
    new ArrowRecordBatch(numRows, rb1_nodes, rb1_bufferlist)
  }

  def toArrowSchema(attributes: Seq[Attribute]): Schema = {
    val fields = attributes.map(attr => {
      Field
        .nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })
    new Schema(fields.toList.asJava)
  }

  def toArrowSchema(schema: StructType): Schema = {
    val fields = schema
      .map(field => {
        Field.nullable(field.name, CodeGeneration.getResultType(field.dataType))
      })
    new Schema(fields.toList.asJava)
  }

  override def toString(): String = {
    s"ConverterUtils"
  }

  @throws[IOException]
  def getSchemaBytesBuf(schema: Schema): Array[Byte] = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema)
    out.toByteArray
  }

  @throws[GandivaException]
  def getExprListBytesBuf(exprs: List[ExpressionTree]): Array[Byte] = {
    val builder: ExpressionList.Builder = GandivaTypes.ExpressionList.newBuilder
    exprs.foreach { expr => builder.addExprs(expr.toProtobuf) }
    builder.build.toByteArray
  }
}
