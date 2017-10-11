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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.{ByteArrayOutputStream, ObjectOutputStream, OutputStream}

import scala.collection.mutable
import scala.collection.JavaConverters._

import com.google.common.collect.ArrayListMultimap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.FromUnsafeProjection
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

private[index] class BitmapIndexRecordWriter(
    configuration: Configuration,
    writer: OutputStream,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private val multiHashMap = ArrayListMultimap.create[InternalRow, Int]()
  private var recordCount: Int = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    multiHashMap.put(v, recordCount)
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    flushToFile(writer)
    writer.close()
  }

  private def flushToFile(out: OutputStream): Unit = {

    val statisticsManager = new StatisticsManager
    statisticsManager.initialize(BitMapIndexType, keySchema, configuration)

    // generate the bitset hashmap
    val hashMap = new mutable.HashMap[InternalRow, BitSet]()

    multiHashMap.asMap().asScala.foreach(kv => {
      val bs = new BitSet(recordCount)
      kv._2.asScala.foreach(bs.set)
      hashMap.put(kv._1, bs)
    })

    val header = writeHead(writer, IndexFile.INDEX_VERSION)
    // serialize hashMap and get length
    val writeBuf = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(writeBuf)
    out.writeObject(hashMap)
    out.flush()
    val objLen = writeBuf.size()
    // write byteArray length and byteArray
    IndexUtils.writeInt(writer, objLen)
    writer.write(writeBuf.toByteArray)
    out.close()
    val indexEnd = 4 + objLen + header
    val offset: Long = indexEnd

    statisticsManager.write(writer)

    // write index file footer
    IndexUtils.writeLong(writer, indexEnd) // statistics start pos
    IndexUtils.writeLong(writer, offset) // index file end offset
    IndexUtils.writeLong(writer, indexEnd) // dataEnd
  }

  private def writeHead(writer: OutputStream, version: Int): Int = {
    writer.write("OAPIDX".getBytes("UTF-8"))
    assert(version <= 65535)
    val data = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(data)
    IndexFile.indexFileHeaderLength
  }
}
