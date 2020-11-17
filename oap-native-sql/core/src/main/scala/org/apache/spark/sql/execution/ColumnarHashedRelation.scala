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

package org.apache.spark.sql.execution

import java.io._
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized.{ArrowWritableColumnVector, SerializableObject}
import org.apache.spark.util.{KnownSizeEstimation, Utils}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import java.util.concurrent.atomic.AtomicBoolean

class ColumnarHashedRelation(
    var hashRelationObj: SerializableObject,
    var arrowColumnarBatch: Array[ColumnarBatch],
    var arrowColumnarBatchSize: Int)
    extends Externalizable
    with KryoSerializable
    with KnownSizeEstimation {
  val refCnt: AtomicInteger = new AtomicInteger()
  val closed: AtomicBoolean = new AtomicBoolean()

  def this() = {
    this(null, null, 0)
  }

  def asReadOnlyCopy(): ColumnarHashedRelation = {
    //new ColumnarHashedRelation(hashRelationObj, arrowColumnarBatch, arrowColumnarBatchSize)
    refCnt.incrementAndGet()
    this
  }

  override def estimatedSize: Long = 0

  def close(waitTime: Int): Future[Int] = Future {
    Thread.sleep(waitTime * 1000)
    if (refCnt.get == 0) {
      if (!closed.getAndSet(true)) {
        hashRelationObj.close
        arrowColumnarBatch.foreach(_.close)
        System.err.println(s"ColumnarHashedRelation close called")
      }
    }
    refCnt.get
  }

  def countDownClose(waitTime: Int = -1): Unit = {
    val curRefCnt = refCnt.decrementAndGet()
    if (waitTime == -1) return
    if (curRefCnt == 0) {
      close(waitTime).onComplete {
        case Success(resRefCnt) => {}
        case Failure(e) =>
          System.err.println(s"Failed to close ColumnarHashedRelation, exception = $e")
      }
    }
  }

  override def finalize(): Unit = {
    if (!closed.getAndSet(true)) {
      hashRelationObj.close
      arrowColumnarBatch.foreach(_.close)
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    if (closed.get()) return
    out.writeObject(hashRelationObj)
    val rawArrowData = ConverterUtils.convertToNetty(arrowColumnarBatch)
    out.writeObject(rawArrowData)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    if (closed.get()) return
    kryo.writeObject(out, hashRelationObj)
    val rawArrowData = ConverterUtils.convertToNetty(arrowColumnarBatch)
    kryo.writeObject(out, rawArrowData)
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashRelationObj = in.readObject().asInstanceOf[SerializableObject]
    val rawArrowData = in.readObject().asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    // retain all cols
    /*arrowColumnarBatch.foreach(cb => {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    })*/
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    hashRelationObj =
      kryo.readObject(in, classOf[SerializableObject]).asInstanceOf[SerializableObject]
    val rawArrowData = kryo.readObject(in, classOf[Array[Byte]]).asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    // retain all cols
    /*arrowColumnarBatch.foreach(cb => {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWr:w
        itableColumnVector].retain())
    })*/
  }

  def size(): Int = {
    hashRelationObj.total_size + arrowColumnarBatchSize
  }

  def getColumnarBatchAsIter: Iterator[ColumnarBatch] = {
    if (closed.get())
      throw new InvalidObjectException(
        s"can't getColumnarBatchAsIter from a deleted ColumnarHashedRelation.")
    new Iterator[ColumnarBatch] {
      var idx = 0
      val total_len = arrowColumnarBatch.length
      override def hasNext: Boolean = idx < total_len
      override def next(): ColumnarBatch = {
        val tmp_idx = idx
        idx += 1
        val cb = arrowColumnarBatch(tmp_idx)
        // retain all cols
        (0 until cb.numCols).toList.foreach(i =>
          cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
        cb
      }
    }
  }

}
