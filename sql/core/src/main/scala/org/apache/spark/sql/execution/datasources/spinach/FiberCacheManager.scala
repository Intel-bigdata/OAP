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

import java.util
import java.util.concurrent.TimeUnit

import com.google.common.cache._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.types.{AtomicType, DataType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

object FiberCacheManager extends Logging {
  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .weigher(new Weigher[Fiber, FiberByteData] {
        override def weigh(key: Fiber, value: FiberByteData): Int = value.buf.length
      })
      .maximumWeight(MemoryManager.SPINACH_FIBER_CACHE_SIZE_IN_BYTES)
      .removalListener(new RemovalListener[Fiber, FiberByteData] {
        override def onRemoval(n: RemovalNotification[Fiber, FiberByteData]): Unit = {
          MemoryManager.instance.free(n.getValue)
        }
      }).build(new CacheLoader[Fiber, FiberByteData] {
      override def load(key: Fiber): FiberByteData = {
        val handler: FiberDataFileHandler = FiberDataFileHandler(key.file)
        // TODO load the data fiber
        val splitMeta = handler.splitMeta
        val rowGroupStartPos = splitMeta.rowGroupMeta(key.rowGroupId)._1
        val fiberLen = splitMeta.rowGroupMeta(key.rowGroupId)._2

        var offset: Int = 0
        var idx: Int = 0
        while(idx < key.columnIndex - 1) {
          offset += fiberLen(idx)
          idx += 1
        }

        val oldPos = handler.is.getPos
        handler.is.seek(rowGroupStartPos + offset)
        val buf = new Array[Byte](fiberLen(idx))
        handler.is.read(buf, 0, fiberLen(idx))
        handler.is.seek(oldPos)

        return FiberByteData(buf)
      }
    })

  def apply(fiberCache: Fiber): FiberByteData = {
    cache(fiberCache)
  }

  // TODO this will be called via heartbeat
  def status: String = throw new NotImplementedError()
  def update(status: String): Unit = throw new NotImplementedError()
}

private[spinach] case class FiberDataFileHandler(is: FSDataInputStream,
                                                 splitMeta: SpinachSplitMeta)

private[spinach] object FiberDataFileHandler extends Logging {
  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .maximumSize(MemoryManager.SPINACH_FIBER_CACHE_SIZE_IN_BYTES)
      .expireAfterAccess(100, TimeUnit.SECONDS) // auto expire after 100 seconds.
      .removalListener(new RemovalListener[DataFile, FiberDataFileHandler] {
        override def onRemoval(n: RemovalNotification[DataFile, FiberDataFileHandler])
        : Unit = {
          n.getValue.is.close()
        }
      }).build(new CacheLoader[DataFile, FiberDataFileHandler] {
      override def load(key: DataFile): FiberDataFileHandler = {
        // TODO make the ctx also cached?
        val ctx = SparkHadoopUtil.get.getConfigurationFromJobContext(key.context)
        val fs = FileSystem.get(ctx)
        val path = new Path(StringUtils.unEscapeString(key.path))
        val status = fs.getFileStatus(path)
        val is = fs.open(path)
        val fileLen = status.getLen
        FiberDataFileHandler(is, SpinachSplitMetaBuilder(is, fileLen))
      }
    })

  def apply(fiberCache: DataFile): FiberDataFileHandler = {
    cache(fiberCache)
  }
}

case class FiberByteData(buf: Array[Byte]) // TODO add FiberDirectByteData

class ColumnValues(dataType: DataType, val raw: FiberByteData) {
  require(dataType.isInstanceOf[AtomicType], "Only atomic type accepted for now.")

  // TODO get the bitset from the FiberByteData
  val bitset: util.BitSet = throw new NotImplementedError()
  def isNullAt(idx: Int): Boolean = bitset.get(idx)

  def getBooleanValue(idx: Int): Boolean = {
    Platform.getBoolean(raw.buf, Platform.BYTE_ARRAY_OFFSET + idx)
  }
  def getByteValue(idx: Int): Byte = {
    Platform.getByte(raw.buf, Platform.BYTE_ARRAY_OFFSET + idx)
  }
  def getShortValue(idx: Int): Short = {
    Platform.getShort(raw.buf, Platform.BYTE_ARRAY_OFFSET + idx)
  }
  def getFloatValue(idx: Int): Float = {
    Platform.getFloat(raw.buf, Platform.BYTE_ARRAY_OFFSET + idx)
  }
  def getInt(idx: Int): Int = {
    Platform.getInt(raw.buf, Platform.BYTE_ARRAY_OFFSET + idx)
  }
  def getDouble(idx: Int): Double = {
    Platform.getDouble(raw.buf, Platform.BYTE_ARRAY_OFFSET + idx)
  }
  def getLong(idx: Int): Long = {
    Platform.getLong(raw.buf, Platform.BYTE_ARRAY_OFFSET + idx)
  }
  def getString(idx: Int): UTF8String = {
    //  The byte data format like:
    //    value #1 length (int)
    //    value #1 offset, (0 - based to the start of this Fiber Group)
    //    value #2 length
    //    value #2 offset, (0 - based to the start of this Fiber Group)
    //    …
    //    …
    //    value #N length
    //    value #N offset, (0 - based to the start of this Fiber Group)
    //    value #1
    //    value #2
    //    …
    //    value #N
    val length = getInt(idx * 2)
    val offset = getInt(idx * 2 + 1)
    UTF8String.fromAddress(raw.buf, Platform.BYTE_ARRAY_OFFSET + offset, length)
  }
  def getBinary(idx: Int): Array[Byte] = {
    //  The byte data format like:
    //    value #1 length (int)
    //    value #1 offset, (0 - based to the start of this Fiber Group)
    //    value #2 length
    //    value #2 offset, (0 - based to the start of this Fiber Group)
    //    …
    //    …
    //    value #N length
    //    value #N offset, (0 - based to the start of this Fiber Group)
    //    value #1
    //    value #2
    //    …
    //    value #N
    val length = getInt(idx * 2)
    val offset = getInt(idx * 2 + 1)
    val result = new Array[Byte](length)
    Platform.copyMemory(raw.buf, Platform.BYTE_ARRAY_OFFSET + offset, result,
      Platform.BYTE_ARRAY_OFFSET, length)

    result
  }
}
