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

import java.io.IOException
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap}
import com.google.common.cache._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.util.StringUtils
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.CustomManager
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.utils.CacheStatusSerDe
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet
import org.apache.spark.unsafe.Platform

// TODO need to register within the SparkContext
class SpinachHeartBeatMessager extends CustomManager with Logging {
  override def status(conf: SparkConf): String = {
    FiberCacheManager.status
  }
}

private[spinach] trait AbstractFiberCacheManger extends Logging {
  protected def fiber2Data(key: Fiber): FiberCacheData

  @transient protected val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .weigher(new Weigher[Fiber, FiberCacheData] {
        override def weigh(key: Fiber, value: FiberCacheData): Int = value.fiberData.size().toInt
      })
      .maximumWeight(MemoryManager.getCapacity())
      .removalListener(new RemovalListener[Fiber, FiberCacheData] {
        override def onRemoval(n: RemovalNotification[Fiber, FiberCacheData]): Unit = {
          MemoryManager.free(n.getValue)
        }
      })
      .build(new CacheLoader[Fiber, FiberCacheData] {
        override def load(key: Fiber): FiberCacheData = {
          fiber2Data(key)
        }
      })

  def apply(fiberCache: Fiber): FiberCacheData = {
    cache.get(fiberCache)
  }

  def status: String = {
    val fiberFileToFiberMap = new HashMap[String, Buffer[Fiber]]()
    val fiberCacheMap = cache.asMap().asScala
    fiberCacheMap.foreach { case (fiber, _) =>
      fiberFileToFiberMap.getOrElseUpdate(fiber.file.path, new ArrayBuffer[Fiber]) += fiber
    }

    val fibers = this.cache.asMap().keySet().asScala
    val statusRawData = fibers.map { fiber =>
      val dataFileScanner = fiber.file
      val fileMeta = dataFileScanner.meta
      val fiberBitSet = new BitSet(fileMeta.groupCount * fileMeta.fieldCount)
      val fiberCachedList: Seq[Fiber] = fiberFileToFiberMap
        .getOrElse(dataFileScanner.path, Seq.empty)
      fiberCachedList.foreach { fiber =>
        fiberBitSet.set(fiber.columnIndex + fileMeta.fieldCount * fiber.rowGroupId)
      }
      FiberCacheStatus(dataFileScanner.path, fiberBitSet, fileMeta)
    }.toSeq

    val retStatus = CacheStatusSerDe.serialize(statusRawData)
    retStatus
  }
}

/**
  * Fiber Cache Manager
  */
object FiberCacheManager extends AbstractFiberCacheManger {
  override def fiber2Data(key: Fiber): FiberCacheData = {
    key.file.getFiberData(key.rowGroupId, key.columnIndex)
  }
}

private[spinach] case class InputDataFileDescriptor(fin: FSDataInputStream, len: Long)

private[spinach] object DataMetaCacheManager extends Logging {
  // TODO: make it configurable
  // temporarily using java options to config.
  val spinachDataMetaCacheSize = System.getProperty("spinach.datametacache.size",
    "262144").toLong  // default size is 256k

  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .maximumSize(spinachDataMetaCacheSize)
      .build(new CacheLoader[DataFileScanner, DataFileMeta] {
      override def load(key: DataFileScanner): DataFileMeta = {
        val fd = FiberDataFileHandler(key)
        new DataFileMeta().read(fd.fin, fd.len)
      }
    })

  def apply(fiberCache: DataFileScanner): DataFileMeta = {
    cache.get(fiberCache)
  }
}

private[spinach] object FiberDataFileHandler extends Logging {
  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .maximumSize(MemoryManager.getCapacity())
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[DataFileScanner, InputDataFileDescriptor] {
        override def onRemoval(n: RemovalNotification[DataFileScanner, InputDataFileDescriptor])
        : Unit = {
          n.getValue.fin.close()
        }
      }).build(new CacheLoader[DataFileScanner, InputDataFileDescriptor] {
      override def load(key: DataFileScanner): InputDataFileDescriptor = {
        val ctx = SparkHadoopUtil.get.getConfigurationFromJobContext(key.context)
        val path = new Path(StringUtils.unEscapeString(key.path))
        val fs = FileSystem.get(ctx)

        InputDataFileDescriptor(fs.open(path), fs.getFileStatus(path).getLen)
      }
    })

  def apply(fiberCache: DataFileScanner): InputDataFileDescriptor = {
    cache.get(fiberCache)
  }

}

private[spinach] case class Fiber(file: DataFileScanner, columnIndex: Int, rowGroupId: Int)

private[spinach] case class DataFileScanner(
    path: String, schema: StructType, context: TaskAttemptContext) {
  lazy val meta: DataFileMeta = DataMetaCacheManager(this)
  // TODO: add SparkConf
  val compCodec = new SnappyCompressionCodec(new SparkConf())

  override def hashCode(): Int = path.hashCode
  override def equals(that: Any): Boolean = that match {
    case DataFileScanner(thatPath, _, _) => path == thatPath
    case _ => false
  }

  def getFiberData(groupId: Int, fiberId: Int): FiberCacheData = {
    val is = FiberDataFileHandler(this).fin
    val groupMeta = meta.rowGroupsMeta(groupId)
    // get the fiber data start position
    // TODO: update the meta to store the fiber start pos
    var i = 0
    var fiberStart = groupMeta.start
    while (i < fiberId) {
      fiberStart += groupMeta.fiberLens(i)
      i += 1
    }
    val len = groupMeta.fiberLens(fiberId)
    val bytes = new Array[Byte](len)

    is.synchronized {
      is.seek(fiberStart)
      is.read(bytes)
      putToFiberCache(bytes)
    }

  }

  def putToFiberCache(buf: Array[Byte]): FiberCacheData = {
    // TODO: make it configurable
    // TODO: disable compress first since there's some issue to solve with conpression
    val newBuf = if (false) {
      compCodec.compressedInputBuffer(buf)
    } else {
      buf
    }
    val fiberCacheData = MemoryManager.allocate(newBuf.length)
    Platform.copyMemory(newBuf, Platform.BYTE_ARRAY_OFFSET, fiberCacheData.fiberData.getBaseObject,
      fiberCacheData.fiberData.getBaseOffset, newBuf.length)
    fiberCacheData
  }

  // full file scan
  def iterator(requiredIds: Array[Int]): Iterator[InternalRow] = {
    val row = new BatchColumn()
    val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)
    (0 until meta.groupCount).iterator.flatMap { groupId =>
      var i = 0
      while (i < columns.length) {
        columns(i) = new ColumnValues(
          meta.rowCountInEachGroup,
          schema(requiredIds(i)).dataType,
          FiberCacheManager(Fiber(this, requiredIds(i), groupId)))
        i += 1
      }

      if (groupId < meta.groupCount - 1) {
        // not the last row group
        row.reset(meta.rowCountInEachGroup, columns).toIterator
      } else {
        row.reset(meta.rowCountInLastGroup, columns).toIterator
      }
    }
  }

  // scan by given row ids
  def iterator(requiredIds: Array[Int], rowIds: Array[Int]): Iterator[InternalRow] = {
    val row = new BatchColumn()
    val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)
    var lastGroupId = -1
    (0 until rowIds.length).iterator.map { idx =>
      val rowId = rowIds(idx)
      val groupId = (rowId + 1) / meta.rowCountInEachGroup
      val rowIdxInGroup = rowId % meta.rowCountInEachGroup

      if (lastGroupId != groupId) {
        // if we didn't move to another row group
        var i = 0
        while (i < columns.length) {
          columns(i) = new ColumnValues(
            meta.rowCountInEachGroup,
            schema(requiredIds(i)).dataType,
            FiberCacheManager(Fiber(this, requiredIds(i), groupId)))
          i += 1
        }
        if (groupId < meta.groupCount - 1) {
          // not the last row group
          row.reset(meta.rowCountInEachGroup, columns)
        } else {
          row.reset(meta.rowCountInLastGroup, columns)
        }
      }

      row.moveToRow(rowIdxInGroup)
    }
  }
}
