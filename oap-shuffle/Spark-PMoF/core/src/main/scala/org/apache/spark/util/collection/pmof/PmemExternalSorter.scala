package org.apache.spark.util.collection.pmof

import java.util.Comparator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.util.collection._
import org.apache.spark.storage.pmof._
import org.apache.spark.util.configuration.pmof.PmofConf

private[spark] class PmemExternalSorter[K, V, C](
    context: TaskContext,
    handle: BaseShuffleHandle[K, _, C],
    pmofConf: PmofConf,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer)
  extends ExternalSorter[K, V, C](context, aggregator, partitioner, ordering, serializer) with Logging {
  private[this] val pmemBlockOutputStreamArray: ArrayBuffer[PmemBlockOutputStream] = ArrayBuffer[PmemBlockOutputStream]()
  private[this] var mapStage = false
  private[this] val dep = handle.dependency
  private[this] val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private[this] val shouldPartition = numPartitions > 1

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  def setPartitionByteBufferArray(writerArray: Array[PmemBlockOutputStream] = null): Unit = {
    for (i <- writerArray.indices) {
      pmemBlockOutputStreamArray += writerArray(i)
    }
    mapStage = true
  }

  def getPartitionByteBufferArray(stageId: Int, partitionId: Int): PmemBlockOutputStream = {
    if (mapStage) {
      pmemBlockOutputStreamArray(partitionId)
    } else {
      pmemBlockOutputStreamArray += new PmemBlockOutputStream(
        context.taskMetrics(),
        PmemBlockId.getTempBlockId(stageId),
        SparkEnv.get.serializerManager,
        serializer,
        SparkEnv.get.conf,
        pmofConf,
        1,
        numPartitions)
      pmemBlockOutputStreamArray(pmemBlockOutputStreamArray.length - 1)
    }
  }

  def forceSpillToPmem(): Boolean = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) getCollection("map") else getCollection("buffer")
    spill(collection)
    true
  }

  override protected[this] def maybeSpill(collection: WritablePartitionedPairCollection[K, C], currentMemory: Long): Boolean = {
    var shouldSpill = false

    if (elementsRead % 32 == 0 && currentMemory >= pmofConf.inMemoryCollectionSizeThreshold) {
      shouldSpill = currentMemory >= pmofConf.inMemoryCollectionSizeThreshold
    }
    if (shouldSpill) {
      spill(collection)
    }
    shouldSpill
  }

  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    spillMemoryIteratorToPmem(inMemoryIterator)
  }

  private[this] def spillMemoryIteratorToPmem(inMemoryIterator: WritablePartitionedIterator): Unit = {
    var buffer: PmemBlockOutputStream = null
    var cur_partitionId = -1
    while (inMemoryIterator.hasNext) {
      val partitionId = inMemoryIterator.nextPartition()
      if (cur_partitionId != partitionId) {
        if (cur_partitionId != -1) {
          buffer.maybeSpill(true)
        }
        cur_partitionId = partitionId
        buffer = getPartitionByteBufferArray(dep.shuffleId, cur_partitionId)
      }
      require(partitionId >= 0 && partitionId < numPartitions,
        s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")

      val elem = if (inMemoryIterator.hasNext) inMemoryIterator.writeNext(buffer) else null
      //elementsPerPartition(partitionId) += 1
    }
    if (buffer != null) {
      buffer.maybeSpill(true)
    }
  }

  override def stop(): Unit = {
    pmemBlockOutputStreamArray.foreach(_.close())
  }

  /**
    * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
    * group together the pairs for each partition into a sub-iterator.
    *
    * @param data an iterator of elements, assumed to already be sorted by partition ID
    */
  private def groupByPartition(data: Iterator[((Int, K), C)])
  : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
    * An iterator that reads only the elements for a given partition ID from an underlying buffered
    * stream, assuming this partition is the next one to be read. Used to make it easier to return
    * partitioned iterators from our in-memory collection.
    */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    var counts: Long = 0
    override def hasNext: Boolean = {
      data.hasNext && data.head._1._1 == partitionId
    }

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      counts += 1
      (elem._1._2, elem._2)
    }
  }

  def getCollection(variableName: String): WritablePartitionedPairCollection[K, C] = {
    import java.lang.reflect._
    // use reflection to get private map or buffer
    val privateField: Field = this.getClass().getSuperclass().getDeclaredField(variableName)
    privateField.setAccessible(true)
    val fieldValue = privateField.get(this)
    fieldValue.asInstanceOf[WritablePartitionedPairCollection[K, C]]
  }

  override def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) getCollection("map") else getCollection("buffer")
    if (pmemBlockOutputStreamArray.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (ordering.isEmpty) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // Merge spilled and in-memory data
      merge(destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  def merge(inMemory: Iterator[((Int, K), C)]): Iterator[(Int, Iterator[Product2[K, C]])] = {
    // this function is used to merge spilled data with inMemory records
    val inMemBuffered = inMemory.buffered
    val readers: ArrayBuffer[SpillReader] = pmemBlockOutputStreamArray.map(pmemBlockOutputStream => {new SpillReader(pmemBlockOutputStream)})
    (0 until numPartitions).iterator.map { partitionId =>
      val inMemIterator = new IteratorForPartition(partitionId, inMemBuffered)
      val iterators = readers.map(_.readPartitionIter(partitionId)) ++ Seq(inMemIterator)

      // may aggregate / sort / just put together
      if (aggregator.isDefined) {
        // Perform partial aggregation across partitions
        (partitionId, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (partitionId, mergeSort(iterators, ordering.get))
      } else {
        (partitionId, iterators.iterator.flatten)
      }
    }
  }
  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K]): Iterator[Product2[K, C]] =
  {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse order because PriorityQueue dequeues the max
      override def compare(x: Iter, y: Iter): Int = comparator.compare(y.head._1, x.head._1)
    })
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = heap.nonEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   */
  def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],
      mergeCombiners: (C, C) => C,
      comparator: Comparator[K],
      totalOrder: Boolean)
      : Iterator[Product2[K, C]] =
  {
    if (!totalOrder) {
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      new Iterator[Iterator[Product2[K, C]]] {
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
      }.flatMap(i => i)
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted: BufferedIterator[Product2[K, C]] = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  class SpillReader(pmemBlockOutputStream: PmemBlockOutputStream) {
    // Each spill reader is relate to one partition
    // which is different from spark original codes (relate to one spill file)
    val pmemBlockInputStream = new PmemBlockInputStream[K, C](pmemBlockOutputStream, serializer)
    var nextItem: (K, C) = _

    def readPartitionIter(partitionId: Int): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = pmemBlockInputStream.readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        getPartition(nextItem._1) == partitionId
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }
  }
}
