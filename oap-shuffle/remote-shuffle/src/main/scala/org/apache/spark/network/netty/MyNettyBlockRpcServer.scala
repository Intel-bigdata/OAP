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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.language.existentials
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.remote.{HadoopFileSegmentManagedBuffer, MessageForHadoopManagedBuffers, RemoteShuffleManager}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class MyNettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        val blocksNum = openBlocks.blockIds.length
        val isShuffleRequest = (blocksNum > 0) &&
          (BlockId.apply(openBlocks.blockIds(0)).isInstanceOf[ShuffleBlockId] ||
            BlockId.apply(openBlocks.blockIds(0)).isInstanceOf[ShuffleBlockBatchId]) &&
          (SparkEnv.get.conf.get("spark.shuffle.manager", classOf[SortShuffleManager].getName)
            == classOf[RemoteShuffleManager].getName)
        if (isShuffleRequest) {
          val blockIdAndManagedBufferPair =
            openBlocks.blockIds.map(block => (block, blockManager.getHostLocalShuffleData(
              BlockId.apply(block), Array.empty).asInstanceOf[HadoopFileSegmentManagedBuffer]))
          responseContext.onSuccess(new MessageForHadoopManagedBuffers(
            blockIdAndManagedBufferPair).toByteBuffer.nioBuffer())
        } else {
          // This customized Netty RPC server is only served for RemoteShuffle requests,
          // Other RPC messages or data chunks transferring should go through
          // NettyBlockTransferService' NettyBlockRpcServer
          throw new UnsupportedOperationException("MyNettyBlockRpcServer only serves remote" +
            " shuffle requests for OpenBlocks")
        }

      case uploadBlock: UploadBlock =>
        throw new UnsupportedOperationException("MyNettyBlockRpcServer doesn't serve UploadBlock")
    }
  }

  override def receiveStream(
      client: TransportClient,
      messageHeader: ByteBuffer,
      responseContext: RpcResponseCallback): StreamCallbackWithID = {
    throw new UnsupportedOperationException("MyNettyBlockRpcServer doesn't support receiving" +
      " stream")
  }

  override def getStreamManager(): StreamManager = streamManager
}
