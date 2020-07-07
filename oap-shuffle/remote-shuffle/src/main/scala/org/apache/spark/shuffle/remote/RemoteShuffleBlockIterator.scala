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

import java.io.{IOException, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.{lang, util}

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS, REDUCER_MAX_REQS_IN_FLIGHT, REDUCER_MAX_SIZE_IN_FLIGHT}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle._
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockException, BlockId, BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.io.ChunkedByteBufferOutputStream
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient   [[ShuffleClient]] for fetching remote blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage. Note that zero-sized blocks are
 *                        already excluded, which happened in
 *                        [[org.apache.spark.MapOutputTracker]].
 * @param streamWrapper   A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param detectCorrupt whether to detect any corruption in fetched blocks.
 */
private[spark]
final class RemoteShuffleBlockIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient,
    resolver: RemoteShuffleBlockResolver,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    detectCorrupt: Boolean,
    readMetrics: ShuffleReadMetricsReporter,
    doBatchFetch: Boolean)

  extends Iterator[(BlockId, InputStream)] with Logging {

  import RemoteShuffleBlockIterator._

  private val indexCacheEnabled = resolver.indexCacheEnabled

  /**
   * Total number of blocks to fetch. This should be equal to the total number of blocks
   * in [[blocksByAddress]] because we already filter out zero-sized blocks in [[blocksByAddress]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[RemoteFetchResult]

  /**
   * Current [[RemoteFetchResult]] being processed. We track this so we can release
   * the current buffer in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessRemoteFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[RemoteFetchRequest]

  /**
   * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[RemoteFetchRequest]]()

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
    * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
    * at most once for those corrupted blocks.
    */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  /**
    * Whether the iterator is still active. If isZombie is true, the callback interface will no
    * longer place fetched blocks into [[results]].
    */
  @GuardedBy("this")
  private[this] var isZombie = false

  initialize()

  /**
    * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
    */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessRemoteFetchResult(_, _, _, _, buf, _) =>
          readMetrics.incRemoteBytesRead(buf.size)
          readMetrics.incRemoteBlocksFetched(1)
        case _ =>
      }
    }
  }

  private[this] def sendRequest(req: RemoteFetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the info of each blockID
    val infoMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    val blockIds = req.blocks.map(_.blockId)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        val casted = buf.asInstanceOf[HadoopFileSegmentManagedBuffer]
        val res = Future {
          // Another possibility: casted.prepareData(results.size == 0) to only eagerly require a
          // Shuffle Block when the results queue is empty
          casted.prepareData(eagerRequirement = eagerRequirement)
        } (RemoteShuffleBlockIterator.executionContext)
        res.onComplete {
          case Success(_) =>
            RemoteShuffleBlockIterator.this.synchronized {
              // Only add the buffer to results queue if the iterator is not zombie,
              // i.e. cleanup() has not been called yet.
              if (!isZombie) {
                remainingBlocks -= blockId
                results.put(SuccessRemoteFetchResult(
                  BlockId(blockId),
                  infoMap(blockId)._2,
                  address,
                  infoMap(blockId)._1,
                  buf,
                  remainingBlocks.isEmpty))
                logDebug("remainingBlocks: " + remainingBlocks)
              }
            }
          case Failure(e) =>
            results.put(FailureRemoteFetchResult(BlockId(blockId), infoMap(blockId)._2, address, e))

        } (RemoteShuffleBlockIterator.executionContext)
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) ", e)
        results.put(FailureRemoteFetchResult(BlockId(blockId), infoMap(blockId)._2, address, e))
      }
    }
    if (indexCacheEnabled) {
      shuffleClient.fetchBlocks(
        address.host, address.port, address.executorId, blockIds.map(_.toString()).toArray,
        blockFetchingListener, null)
    } else {
      fetchBlocks(blockIds.toArray, blockFetchingListener)
    }
  }

  private def fetchBlocks(
      blockIds: Array[BlockId],
      listener: BlockFetchingListener) = {
    for (blockId <- blockIds) {
      // Note by Chenzhao: Can be optimized by reading consecutive blocks
      try {
        val buf = resolver.getBlockData(blockId)
        listener.onBlockFetchSuccess(blockId.toString(), buf)
      } catch {
        case e: Exception => listener.onBlockFetchFailure(blockId.toString(), e)
      }
    }
  }

  // For remote shuffling, all blocks are remote, so this actually resembles RemoteFetchRequests
  // This function is actually the most like [[collectFetchRequests]] of [[ShuffleBlockFetcherIterator]]
  // in Spark 3.0, which serves for remote fetch requests preparing. This fashion is the only one in
  // this project.
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[RemoteFetchRequest] = {

    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytes InFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[RemoteFetchRequest]

    for ((address, blockInfos) <- blocksByAddress) {
      val iterator = blockInfos.iterator
      var curRequestSize = 0L
      var curBlocks = new ArrayBuffer[FetchBlockInfo]
      while (iterator.hasNext) {
        val (blockId, size, mapIndex) = iterator.next()
        if (size < 0) {
          throw new BlockException(blockId, "Negative block size " + size)
        } else if (size == 0) {
          throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
        } else {
          curBlocks += FetchBlockInfo(blockId, size, mapIndex)
          curRequestSize += size
        }
        // For batch fetch, the actual block in flight should count for merged block.
        val mayExceedsMaxBlocks = !doBatchFetch && curBlocks.size >= maxBlocksInFlightPerAddress
        if (curRequestSize >= targetRequestSize || mayExceedsMaxBlocks) {
          curBlocks = createFetchRequests(curBlocks, address, isLast = false,
            remoteRequests).to[ArrayBuffer]
          curRequestSize = curBlocks.map(_.size).sum
        }
      }
      // Add in the final request
      if (curBlocks.nonEmpty) {
        curBlocks = createFetchRequests(curBlocks, address, isLast = false,
          remoteRequests).to[ArrayBuffer]
        curRequestSize = curBlocks.map(_.size).sum
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks, " +
      s"number of remote requests: ${remoteRequests.size}")
    remoteRequests
  }

  private def createFetchRequests(
      curBlocks: Seq[FetchBlockInfo],
      address: BlockManagerId,
      isLast: Boolean,
      collectedRemoteRequests: ArrayBuffer[RemoteFetchRequest]): Seq[FetchBlockInfo] = {
    val mergedBlocks = mergeContinuousShuffleBlockIdsIfNeeded(curBlocks, doBatchFetch)
    numBlocksToFetch += mergedBlocks.size
    var retBlocks = Seq.empty[FetchBlockInfo]
    if (mergedBlocks.length <= maxBlocksInFlightPerAddress) {
      collectedRemoteRequests += createFetchRequest(mergedBlocks, address)
    } else {
      mergedBlocks.grouped(maxBlocksInFlightPerAddress).foreach { blocks =>
        if (blocks.length == maxBlocksInFlightPerAddress || isLast) {
          collectedRemoteRequests += createFetchRequest(blocks, address)
        } else {
          // The last group does not exceed `maxBlocksInFlightPerAddress`. Put it back
          // to `curBlocks`.
          retBlocks = blocks
          numBlocksToFetch -= blocks.size
        }
      }
    }
    retBlocks
  }

  private def createFetchRequest(
      blocks: Seq[FetchBlockInfo],
      address: BlockManagerId): RemoteFetchRequest = {
    logDebug(s"Creating fetch request of ${blocks.map(_.size).sum} at $address "
      + s"with ${blocks.size} blocks")
    RemoteFetchRequest(address, blocks)
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener[Unit](_ => cleanup())

    // Split local and remote blocks. Actually it assembles remote fetch requests due to all blocks
    // are remote under remote shuffle
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size

  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: RemoteFetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.nanoTime()
      result = results.take()
      val fetchWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait)
      readMetrics.incFetchWaitTime(fetchWaitTime)


      result match {
        case r @SuccessRemoteFetchResult(
        blockId, mapIndex, address, size, buf, isNetworkReqDone) =>
          numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
          readMetrics.incRemoteBytesRead(buf.size())
          readMetrics.incRemoteBlocksFetched(1)
          bytesInFlight -= size
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }
          if (buf.size == 0) {
            // We will never legitimately receive a zero-size block. All blocks with zero records
            // have zero size and all zero-size blocks have no records (and hence should never
            // have been requested in the first place). This statement relies on behaviors of the
            // shuffle writers, which are guaranteed by the following test cases:
            //
            // - BypassMergeSortShuffleWriterSuite: "write with some empty partitions"
            // - UnsafeShuffleWriterSuite: "writeEmptyIterator"
            // - DiskBlockObjectWriterSuite: "commit() and close() without ever opening or writing
            //
            // There is not an explicit test for SortShuffleWriter but the underlying APIs that
            // uses are shared by the UnsafeShuffleWriter (both writers use DiskBlockObjectWriter
            // which returns a zero-size from commitAndGet() in case no records were written
            // since the last call.
            val msg = s"Received a zero-size buffer for block $blockId from $address " +
              s"(expectedApproxSize = $size, isNetworkReqDone=$isNetworkReqDone)"
            throwFetchFailedException(blockId, mapIndex, address, new IOException(msg))
          }
          val in = try {
            buf.createInputStream()
          } catch {
            case e: IOException =>
              // Actually here we know the buf is a HadoopFileSegmentManagedBuffer
              logError("Failed to create input stream from block backed by Hadoop file segment", e)
              throwFetchFailedException(blockId, mapIndex, address, e)
          }
          var isStreamCopied: Boolean = false
          // Detect ShuffleBlock corruption
          try {
            input = streamWrapper(blockId, in)
            // Only copy the stream if it's wrapped by compression or encryption, also the size of
            // block is small (the decompressed block is smaller than maxBytesInFlight)
            if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
              isStreamCopied = true
              val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(input, out, closeStreams = true)
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            }
          } catch {
            case e: IOException =>
              if (corruptedBlocks.contains(blockId)) {
                throwFetchFailedException(blockId, mapIndex, address, e)
              } else {
                logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                corruptedBlocks += blockId
                fetchRequests += RemoteFetchRequest(address, Array(FetchBlockInfo(blockId, size, mapIndex)))
                result = null
              }
          } finally {
            if (isStreamCopied) {
              in.close()
            }
          }
        case FailureRemoteFetchResult(blockId, mapIndex, address, e) =>
          throwFetchFailedException(blockId, mapIndex, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessRemoteFetchResult]
    (currentResult.blockId, input)
  }

  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
          !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue =
          deferredFetchRequests.getOrElse(remoteAddress, new Queue[RemoteFetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }

  }

  private def send(remoteAddress: BlockManagerId, request: RemoteFetchRequest): Unit = {
    sendRequest(request)
    numBlocksInFlightPerAddress(remoteAddress) =
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
  }

  private def isRemoteBlockFetchable(fetchReqQueue: Queue[RemoteFetchRequest]): Boolean = {
    fetchReqQueue.nonEmpty &&
      (bytesInFlight == 0 ||
        (reqsInFlight + 1 <= maxReqsInFlight &&
          bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
  }

  // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
  // given remote address.
  private def isRemoteAddressMaxedOut(
      remoteAddress: BlockManagerId, request: RemoteFetchRequest): Boolean = {
    numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
      maxBlocksInFlightPerAddress
  }


  private def throwFetchFailedException(
    blockId: BlockId, mapIndex: Int, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        // Suppress the BlockManagerId to only retry the failed map tasks, instead of all map tasks
        // that shared the same executor with the failed map tasks. This is more reasonable in
        // remote shuffle
        throw new FetchFailedException(
          null, shufId.toInt, mapId, mapIndex, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

private[remote] object RemoteShuffleBlockIterator {

  private val maxConcurrentFetches =
    RemoteShuffleManager.getConf.get(RemoteShuffleConf.NUM_CONCURRENT_FETCH)
  private val eagerRequirement =
    RemoteShuffleManager.getConf.get(RemoteShuffleConf.DATA_FETCH_EAGER_REQUIREMENT)

  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("shuffle-data-fetching", maxConcurrentFetches))

  /**
   * The block information to fetch used in FetchRequest.
   * @param blockId block id
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage.
   */
  private[remote] case class FetchBlockInfo(blockId: BlockId, size: Long, mapIndex: Int)

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class RemoteFetchRequest(address: BlockManagerId, blocks: Seq[FetchBlockInfo]) {
    val size = blocks.map(_.size).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[remote] sealed trait RemoteFetchResult {
    val blockId: BlockId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param buf `ManagedBuffer` for the content.
   */
  private[remote] case class SuccessRemoteFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends RemoteFetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param e the failure exception
   */
  private[remote] case class FailureRemoteFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable) extends RemoteFetchResult

  /**
   * This function is used to merged blocks when doBatchFetch is true. Blocks which have the
   * same `mapId` can be merged into one block batch. The block batch is specified by a range
   * of reduceId, which implies the continuous shuffle blocks that we can fetch in a batch.
   * For example, input blocks like (shuffle_0_0_0, shuffle_0_0_1, shuffle_0_1_0) can be
   * merged into (shuffle_0_0_0_2, shuffle_0_1_0_1), and input blocks like (shuffle_0_0_0_2,
   * shuffle_0_0_2, shuffle_0_0_3) can be merged into (shuffle_0_0_0_4).
   *
   * @param blocks blocks to be merged if possible. May contains already merged blocks.
   * @param doBatchFetch whether to merge blocks.
   * @return the input blocks if doBatchFetch=false, or the merged blocks if doBatchFetch=true.
   */
  def mergeContinuousShuffleBlockIdsIfNeeded(
      blocks: Seq[FetchBlockInfo],
      doBatchFetch: Boolean): Seq[FetchBlockInfo] = {
    val result = if (doBatchFetch) {
      var curBlocks = new ArrayBuffer[FetchBlockInfo]
      val mergedBlockInfo = new ArrayBuffer[FetchBlockInfo]

      def mergeFetchBlockInfo(toBeMerged: ArrayBuffer[FetchBlockInfo]): FetchBlockInfo = {
        val startBlockId = toBeMerged.head.blockId.asInstanceOf[ShuffleBlockId]

        // The last merged block may comes from the input, and we can merge more blocks
        // into it, if the map id is the same.
        def shouldMergeIntoPreviousBatchBlockId =
          mergedBlockInfo.last.blockId.asInstanceOf[ShuffleBlockBatchId].mapId == startBlockId.mapId

        val (startReduceId, size) =
          if (mergedBlockInfo.nonEmpty && shouldMergeIntoPreviousBatchBlockId) {
            // Remove the previous batch block id as we will add a new one to replace it.
            val removed = mergedBlockInfo.remove(mergedBlockInfo.length - 1)
            (removed.blockId.asInstanceOf[ShuffleBlockBatchId].startReduceId,
              removed.size + toBeMerged.map(_.size).sum)
          } else {
            (startBlockId.reduceId, toBeMerged.map(_.size).sum)
          }

        FetchBlockInfo(
          ShuffleBlockBatchId(
            startBlockId.shuffleId,
            startBlockId.mapId,
            startReduceId,
            toBeMerged.last.blockId.asInstanceOf[ShuffleBlockId].reduceId + 1),
          size,
          toBeMerged.head.mapIndex)
      }

      val iter = blocks.iterator
      while (iter.hasNext) {
        val info = iter.next()
        // It's possible that the input block id is already a batch ID. For example, we merge some
        // blocks, and then make fetch requests with the merged blocks according to "max blocks per
        // request". The last fetch request may be too small, and we give up and put the remaining
        // merged blocks back to the input list.
        if (info.blockId.isInstanceOf[ShuffleBlockBatchId]) {
          mergedBlockInfo += info
        } else {
          if (curBlocks.isEmpty) {
            curBlocks += info
          } else {
            val curBlockId = info.blockId.asInstanceOf[ShuffleBlockId]
            val currentMapId = curBlocks.head.blockId.asInstanceOf[ShuffleBlockId].mapId
            if (curBlockId.mapId != currentMapId) {
              mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
              curBlocks.clear()
            }
            curBlocks += info
          }
        }
      }
      if (curBlocks.nonEmpty) {
        mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
      }
      mergedBlockInfo
    } else {
      blocks
    }
    result
  }

}
