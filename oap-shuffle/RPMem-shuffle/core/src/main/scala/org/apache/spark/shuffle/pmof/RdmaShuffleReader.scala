package org.apache.spark.shuffle.pmof

import org.apache.spark._
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.network.pmof.PmofTransferService
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.pmof._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.collection.pmof.PmemExternalSorter
import org.apache.spark.util.configuration.pmof.PmofConf

/**
  * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
  * requesting them from other nodes' block stores.
  */
private[spark] class RdmaShuffleReader[K, C](handle: BaseShuffleHandle[K, _, C],
                                             startPartition: Int,
                                             endPartition: Int,
                                             context: TaskContext,
                                             pmofConf: PmofConf,
                                             serializerManager: SerializerManager = SparkEnv.get.serializerManager,
                                             blockManager: BlockManager = SparkEnv.get.blockManager,
                                             mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private[this] val dep = handle.dependency
  private[this] val serializerInstance: SerializerInstance = dep.serializer.newInstance()
  private[this] val enable_pmem: Boolean = SparkEnv.get.conf.getBoolean("spark.shuffle.pmof.enable_pmem", defaultValue = true)

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val wrappedStreams: RdmaShuffleBlockFetcherIterator = new RdmaShuffleBlockFetcherIterator(
      context,
      PmofTransferService.getTransferServiceInstance(pmofConf, blockManager),
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", defaultValue = true))


    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (_, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        if (enable_pmem) {
          val sorter = new PmemExternalSorter[K, C, C](context, handle, pmofConf, ordering = Some(keyOrd), serializer = dep.serializer)
          sorter.insertAll(aggregatedIter)
          CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
        } else {
          val sorter =
            new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
          sorter.insertAll(aggregatedIter)
          context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
          context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
          context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
          CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
        }
      case None =>
        aggregatedIter
    }
  }
}
