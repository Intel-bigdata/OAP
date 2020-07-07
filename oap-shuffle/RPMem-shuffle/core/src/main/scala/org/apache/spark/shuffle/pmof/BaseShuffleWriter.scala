package org.apache.spark.shuffle.pmof

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

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.network.pmof.PmofTransferService
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.configuration.pmof.PmofConf
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.BlockManager

private[spark] class BaseShuffleWriter[K, V, C](shuffleBlockResolver: IndexShuffleBlockResolver,
                                                metadataResolver: MetadataResolver,
                                                blockManager: BlockManager,
                                                serializerManager: SerializerManager,
                                                handle: BaseShuffleHandle[K, V, C],
                                                mapId: Long,
                                                context: TaskContext,
                                                pmofConf: PmofConf)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics
  private var sorter: ExternalSorter[K, V, _] = _
  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private var mapStatus: MapStatus = _

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)

      val shuffleServerId = blockManager.shuffleServerId
      if (pmofConf.enableRdma) {
        metadataResolver.pushFileBlockInfo(dep.shuffleId, mapId, partitionLengths)
        val blockManagerId: BlockManagerId =
          BlockManagerId(shuffleServerId.executorId, PmofTransferService.shuffleNodesMap(shuffleServerId.host),
            PmofTransferService.getTransferServiceInstance(pmofConf, blockManager).port, shuffleServerId.topologyInfo)
        mapStatus = MapStatus(blockManagerId, partitionLengths, mapId)
      } else {
        mapStatus = MapStatus(shuffleServerId, partitionLengths, mapId)
      }
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}
