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

package org.apache.spark.shuffle.remote

import java.io._
import java.nio.{ByteBuffer, LongBuffer}
import java.util.UUID
import java.util.function.Consumer

import scala.collection.mutable
import com.google.common.cache.{CacheBuilder, CacheLoader, Weigher}
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.BLOCK_MANAGER_PORT
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.RemoteShuffleTransferService
import org.apache.spark.network.shuffle.ShuffleIndexRecord
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId, TempLocalBlockId, TempShuffleBlockId}
import org.apache.spark.util.Utils

/**
  * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
  * It also manages the resource cleaning and temporary files creation,
  * like a [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] ++
  * [[org.apache.spark.storage.DiskBlockManager]]
  *
  */
class RemoteShuffleBlockResolver(conf: SparkConf) extends ShuffleBlockResolver with Logging {

  private val master = conf.get(RemoteShuffleConf.STORAGE_MASTER_URI)
  private val rootDir = conf.get(RemoteShuffleConf.SHUFFLE_FILES_ROOT_DIRECTORY)
  // 1. Use lazy evaluation due to at the time this class(and its fields) is initialized,
  // SparkEnv._conf is not yet set
  // 2. conf.getAppId may not always work, because during unit tests we may just new a Resolver
  // instead of getting one from the ShuffleManager referenced by SparkContext
  private lazy val applicationId =
    if (Utils.isTesting) s"test${UUID.randomUUID()}" else conf.getAppId
  private def dirPrefix = s"$master/$rootDir/$applicationId"

  // This referenced is shared for all the I/Os with shuffling storage system
  lazy val fs = new Path(dirPrefix).getFileSystem(RemoteShuffleManager.active.getHadoopConf)

  private[remote] lazy val remoteShuffleTransferService: BlockTransferService = {
    val env = SparkEnv.get
    new RemoteShuffleTransferService(
      conf,
      env.securityManager,
      env.blockManager.blockManagerId.host,
      env.blockManager.blockManagerId.host,
      env.conf.get(BLOCK_MANAGER_PORT),
      conf.get(RemoteShuffleConf.NUM_TRANSFER_SERVICE_THREADS))
  }
  private[remote] lazy val shuffleServerId = {
    if (indexCacheEnabled) {
      remoteShuffleTransferService.asInstanceOf[RemoteShuffleTransferService].getShuffleServerId
    } else {
      SparkEnv.get.blockManager.blockManagerId
    }
  }

  private[remote] val indexCacheEnabled: Boolean = {
    val size = JavaUtils.byteStringAsBytes(conf.get(RemoteShuffleConf.REMOTE_INDEX_CACHE_SIZE))
    val dynamicAllocationEnabled =
      conf.getBoolean("spark.dynamicAllocation.enabled", false)
    (size > 0) && {
      if (dynamicAllocationEnabled) {
        logWarning("Index cache is not enabled due to dynamic allocation is enabled, the" +
            " cache in executors may get removed. ")
      }
      !dynamicAllocationEnabled
    }
  }

  if (indexCacheEnabled) {
    logWarning("Fetching index files from the cache of executors which wrote them")
  }

  // These 3 fields will only be initialized when index cache enabled
  lazy val indexCacheSize: String =
    conf.get("spark.shuffle.remote.index.cache.size", "30m")

  lazy val indexCacheLoader: CacheLoader[Path, RemoteShuffleIndexInfo] =
    new CacheLoader[Path, RemoteShuffleIndexInfo]() {
      override def load(file: Path) = new RemoteShuffleIndexInfo(file)
  }

  lazy val shuffleIndexCache =
    CacheBuilder.newBuilder
      .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
      .weigher(new Weigher[Path, RemoteShuffleIndexInfo]() {
        override def weigh(file: Path, indexInfo: RemoteShuffleIndexInfo): Int =
          indexInfo.getSize
      })
      .build(indexCacheLoader)

  def getDataFile(shuffleId: Int, mapId: Long): Path = {
    new Path(s"${dirPrefix}/${shuffleId}_${mapId}.data")
  }

  def getIndexFile(shuffleId: Int, mapId: Long): Path = {
    new Path(s"${dirPrefix}/${shuffleId}_${mapId}.index")
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Long,
      lengths: Array[Long],
      dataTmp: Path): Unit = {

    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = RemoteShuffleUtils.tempPathWith(indexFile)
    try {
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && fs.exists(dataTmp)) {
            fs.delete(dataTmp, true)
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          val out = new DataOutputStream(new BufferedOutputStream(fs.create(indexTmp)))
          val offsetsBuffer = new Array[Long](lengths.length + 1)
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            var offset = 0L
            offsetsBuffer(0) = 0
            out.writeLong(0)
            var i = 1
            for (length <- lengths) {
              offset += length
              offsetsBuffer(i) = offset
              out.writeLong(offset)
              i += 1
            }
          } {
            out.close()
          }
          // Put index info in cache if enabled
          if (indexCacheEnabled) {
            shuffleIndexCache.put(indexFile, new RemoteShuffleIndexInfo(offsetsBuffer))
          }
          if (fs.exists(indexFile)) {
            fs.delete(indexFile, true)
          }
          if (fs.exists(dataFile)) {
            fs.delete(dataFile, true)
          }
          if (!fs.rename(indexTmp, indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && fs.exists(dataTmp) && !fs.rename(dataTmp, dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      if (fs.exists(indexTmp) && !fs.delete(indexTmp, true)) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getName}")
      }
    }
  }

  /**
    * Check whether the given index and data files match each other.
    * If so, return the partition lengths in the data file. Otherwise return null.
    */
  private def checkIndexAndDataFile(index: Path, data: Path, blocks: Int): Array[Long] = {

    // the index file should exist(of course) and have `block + 1` longs as offset.
    if (!fs.exists(index) || fs.getFileStatus(index).getLen != (blocks + 1) * 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      // Note by Chenzhao: originally [[NioBufferedFileInputStream]] is used
      new DataInputStream(new BufferedInputStream(fs.open(index)))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    if (fs.exists(data) && fs.getFileStatus(data).getLen == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  def getBlockData(blockId: BlockId, dirs: Option[Array[String]] = None): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }

    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(shuffleId, mapId)

    val (offset, length) =
      if (indexCacheEnabled) {
        val shuffleIndexInfo = shuffleIndexCache.get(indexFile)
        val range = shuffleIndexInfo.getIndex(startReduceId, endReduceId)
        (range.getOffset, range.getLength)
      } else {
        // SPARK-22982: if this FileInputStream's position is seeked forward by another
        // piece of code which is incorrectly using our file descriptor then this code
        // will fetch the wrong offsets (which may cause a reducer to be sent a different
        // reducer's data). The explicit position checks added here were a useful debugging
        // aid during SPARK-22982 and may help prevent this class of issue from re-occurring
        // in the future which is why they are left here even though SPARK-22982 is fixed.
        val in = fs.open(indexFile)
        in.seek(startReduceId * 8L)
        try {
          val startOffset = in.readLong()
          in.seek(endReduceId * 8L)
          val endOffset = in.readLong()
          val actualPosition = in.getPos()
          val expectedPosition = endReduceId * 8L + 8
          if (actualPosition != expectedPosition) {
            throw new Exception(s"SPARK-22982: Incorrect channel position " +
              s"after index file reads: expected $expectedPosition but actual" +
              s" position was $actualPosition.")
          }
          (startOffset, endOffset - startOffset)
        } finally {
          in.close()
        }
      }
    new HadoopFileSegmentManagedBuffer(
      getDataFile(shuffleId, mapId),
      offset,
      length)
  }

  /**
    * Remove data file and index file that contain the output data from one map.
    */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (fs.exists(file)) {
      if (!fs.delete(file, true)) {
        logWarning(s"Error deleting data ${file.toString}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (fs.exists(file)) {
      if (!fs.delete(file, true)) {
        logWarning(s"Error deleting index ${file.getName()}")
      }
    }
  }

  def createTempShuffleBlock(): (TempShuffleBlockId, Path) = {
    RemoteShuffleUtils.createTempShuffleBlock(dirPrefix)
  }

  def createTempLocalBlock(): (TempLocalBlockId, Path) = {
    RemoteShuffleUtils.createTempLocalBlock(dirPrefix)
  }

  // Mainly for tests, similar to [[DiskBlockManager.getAllFiles]]
  def getAllFiles(): Seq[Path] = {
    val dir = new Path(dirPrefix)
    val internalIter = fs.listFiles(dir, true)
    new Iterator[Path] {
      override def hasNext: Boolean = internalIter.hasNext

      override def next(): Path = internalIter.next().getPath
    }.toSeq
  }

  override def stop(): Unit = {
    val dir = new Path(dirPrefix)
    fs.delete(dir, true)
    try {
      HadoopFileSegmentManagedBuffer.handleCache.values().forEach {
        new Consumer[mutable.HashMap[Path, FSDataInputStream]] {
          override def accept(t: mutable.HashMap[Path, FSDataInputStream]): Unit = {
            t.values.foreach(JavaUtils.closeQuietly)
          }
        }
      }
      JavaUtils.closeQuietly(remoteShuffleTransferService)
    } catch {
      case e: Exception => logInfo(s"Exception thrown when closing " +
        s"RemoteShuffleTransferService\n" +
          s"Caused by: ${e.toString}\n${e.getStackTrace.mkString("\n")}")
    }
  }
}

// For index cache feature, this is the data structure stored in Guava cache
private[remote] class RemoteShuffleIndexInfo extends Logging {

  private var offsets: LongBuffer = _
  private var size: Int = _

  // Construction by reading index files from storage to memory, which happens in reduce stage
  def this(indexFile: Path) {
    this()
    val fs = RemoteShuffleManager.getFileSystem

    size = fs.getFileStatus(indexFile).getLen.toInt
    val rawBuffer = ByteBuffer.allocate(size)
    offsets = rawBuffer.asLongBuffer
    var input: FSDataInputStream = null
    try {
      logInfo("Loading index file from storage to Guava cache")
      input = fs.open(indexFile)
      input.readFully(rawBuffer.array)
    } finally {
      if (input != null) {
        input.close()
      }
    }
  }

  // Construction by directly putting the index offsets info in cache, which happens in map stage
  def this(offsetsArray: Array[Long]) {
    this()
    size = offsetsArray.length * 8
    offsets = LongBuffer.wrap(offsetsArray)
  }

  def getSize: Int = size

  /**
    * Get index offset for a particular reducer.
    */
  def getIndex(startReduceId: Int, endReduceId: Int): ShuffleIndexRecord = {
    val offset = offsets.get(startReduceId)
    val nextOffset = offsets.get(endReduceId)
    new ShuffleIndexRecord(offset, nextOffset - offset)
  }
}
