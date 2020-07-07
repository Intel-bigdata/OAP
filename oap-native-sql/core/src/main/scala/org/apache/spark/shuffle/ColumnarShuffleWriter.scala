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

package org.apache.spark.shuffle

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.nio.ByteBuffer

import com.google.common.annotations.VisibleForTesting
import com.google.common.io.Closeables
import com.intel.oap.vectorized.{
  ArrowWritableColumnVector,
  ShuffleSplitterJniWrapper
}
import org.apache.arrow.util.SchemaUtils
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import scala.collection.mutable.ListBuffer

class ColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
    extends ShuffleWriter[K, V]
    with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val conf = SparkEnv.get.conf

  private val blockManager = SparkEnv.get.blockManager

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = _

  private val transeferToEnabled = conf.getBoolean("spark.file.transferTo", true)
  private val compressionEnabled = conf.getBoolean("spark.shuffle.compress", true)
  private val compressionCodec = conf.get("spark.io.compression.codec", "lz4")
  private val nativeBufferSize =
    conf.getLong("spark.sql.execution.arrow.maxRecordsPerBatch", 4096)

  private val jniWrapper = new ShuffleSplitterJniWrapper()

  private var nativeSplitter: Long = 0

  private var partitionLengths: Array[Long] = _

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, null)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    if (nativeSplitter == 0) {
      val schema: Schema = Schema.deserialize(ByteBuffer.wrap(dep.serializedSchema))
      val localDirs = Utils.getConfiguredLocalDirs(conf).mkString(",")
      nativeSplitter =
        jniWrapper.make(SchemaUtils.get.serialize(schema), nativeBufferSize, localDirs)
      if (compressionEnabled) {
        jniWrapper.setCompressionCodec(nativeSplitter, compressionCodec)
      }
    }

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val bufAddrs = new ListBuffer[Long]()
        val bufSizes = new ListBuffer[Long]()
        (0 until cb.numCols).foreach { idx =>
          val column = cb.column(idx).asInstanceOf[ArrowWritableColumnVector]
          column.getValueVector
            .getBuffers(false)
            .foreach { buffer =>
              bufAddrs += buffer.memoryAddress()
              bufSizes += buffer.readableBytes()
            }
        }
        dep.dataSize.add(bufSizes.sum)

        val startTime = System.nanoTime()
        jniWrapper.split(nativeSplitter, cb.numRows, bufAddrs.toArray, bufSizes.toArray)
        dep.splitTime.add(System.nanoTime() - startTime)
        writeMetrics.incRecordsWritten(1)
      }
    }

    val startTime = System.nanoTime()
    jniWrapper.stop(nativeSplitter)
    dep.splitTime.add(System.nanoTime() - startTime)
    writeMetrics.incBytesWritten(jniWrapper.getTotalBytesWritten(nativeSplitter))

    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      partitionLengths = writePartitionedFile(tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {

    try {
      if (stopping) {
        None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      // delete the temporary files hold by native splitter
      if (nativeSplitter != 0) {
        try {
          jniWrapper.getPartitionFileInfo(nativeSplitter).foreach { fileInfo =>
            {
              val pid = fileInfo.getPid
              val file = new File(fileInfo.getFilePath)
              if (file.exists()) {
                if (!file.delete()) {
                  logError(s"Unable to delete file for partition ${pid}")
                }
              }
            }
          }
        } finally {
          jniWrapper.close(nativeSplitter)
          nativeSplitter = 0
        }
      }
    }
  }

  @throws[IOException]
  private def writePartitionedFile(outputFile: File): Array[Long] = {

    val lengths = new Array[Long](dep.partitioner.numPartitions)
    val out = new FileOutputStream(outputFile, true)
    val writerStartTime = System.nanoTime()
    var threwException = true

    try {
      jniWrapper.getPartitionFileInfo(nativeSplitter).foreach { fileInfo =>
        {
          val pid = fileInfo.getPid
          val filePath = fileInfo.getFilePath

          val file = new File(filePath)
          if (file.exists()) {
            val in = new FileInputStream(file)
            var copyThrewException = true

            try {
              lengths(pid) = Utils.copyStream(in, out, false, transeferToEnabled)
              copyThrewException = false
            } finally {
              Closeables.close(in, copyThrewException)
            }
            if (!file.delete()) {
              logError(s"Unable to delete file for partition ${pid}")
            } else {
              logDebug(s"Deleting temporary shuffle file ${filePath} for partition ${pid}")
            }
          } else {
            logWarning(
              s"Native shuffle writer temporary file ${filePath} for partition ${pid} not exists")
          }
        }
      }
      threwException = false
    } finally {
      Closeables.close(out, threwException)
      writeMetrics.incWriteTime(System.nanoTime - writerStartTime)
    }
    lengths
  }

  @VisibleForTesting
  def getPartitionLengths: Array[Long] = partitionLengths

}
