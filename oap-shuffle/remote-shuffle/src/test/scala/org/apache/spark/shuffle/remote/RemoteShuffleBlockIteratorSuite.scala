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

import java.io.{InputStream, IOException}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.RemoteShuffleTransferService
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage._
import org.apache.spark.util.Utils

class RemoteShuffleBlockIteratorSuite extends SparkFunSuite with LocalSparkContext {

  // With/without index cache, configurations set/unset
  testWithMultiplePath("basic read")(basicRead)

  test("retry corrupt blocks") {
    // To set an active ShuffleManager
    new RemoteShuffleManager(createDefaultConfWithIndexCacheEnabled(true))
    val blockResolver = mock(classOf[RemoteShuffleBlockResolver])
    when(blockResolver.indexCacheEnabled).thenReturn(true)

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    val corruptLocalBuffer = mock(classOf[HadoopFileSegmentManagedBuffer])
    doNothing().when(corruptLocalBuffer).prepareData(any())
    when(corruptLocalBuffer.createInputStream()).thenThrow(new RuntimeException("oops"))

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
            // Return the first block, and then fail.
            listener.onBlockFetchSuccess(
              ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
            listener.onBlockFetchSuccess(
              ShuffleBlockId(0, 1, 0).toString, mockCorruptBuffer())
            listener.onBlockFetchSuccess(
              ShuffleBlockId(0, 2, 0).toString, corruptLocalBuffer)
        }
      })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq)).toIterator

    val taskContext = TaskContext.empty()
    val iterator = new RemoteShuffleBlockIterator(
      taskContext,
      transfer,
      blockResolver,
      blocksByAddress,
      (_, in) => new LimitedInputStream(in, 100),
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      true)

    // The first block should be returned without an exception
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 0))

    // The next block is corrupt local block (the second one is corrupt and retried)
    intercept[FetchFailedException] { iterator.next() }

    intercept[FetchFailedException] { iterator.next() }
  }

  // Create a mock managed buffer for testing
  private def createMockManagedBuffer(size: Int = 1): ManagedBuffer = {
    val mockManagedBuffer = mock(classOf[HadoopFileSegmentManagedBuffer])
    val in = mock(classOf[InputStream])
    when(in.read(any[Array[Byte]])).thenReturn(1)
    when(in.read(any(), any(), any())).thenReturn(1)
    doNothing().when(mockManagedBuffer).prepareData(any())
    when(mockManagedBuffer.createInputStream()).thenReturn(in)
    when(mockManagedBuffer.size()).thenReturn(size)
    mockManagedBuffer
  }

  private def mockCorruptBuffer(size: Long = 1L): ManagedBuffer = {
    val corruptStream = mock(classOf[InputStream])
    when(corruptStream.read(any(), any(), any())).thenThrow(new IOException("corrupt"))
    val corruptBuffer = mock(classOf[HadoopFileSegmentManagedBuffer])
    when(corruptBuffer.size()).thenReturn(size)
    when(corruptBuffer.createInputStream()).thenReturn(corruptStream)
    corruptBuffer
  }

  private def testWithMultiplePath(name: String, loadDefaults: Boolean = true)
      (body: (SparkConf => Unit)): Unit = {
    val indexCacheDisabledConf = createDefaultConf(loadDefaults)
    val indexCacheEnabledConf = createDefaultConfWithIndexCacheEnabled(loadDefaults)

    test(name + " w/o index cache") {
      body(indexCacheDisabledConf)
    }
    test(name + " w/ index cache") {
      body(indexCacheEnabledConf)
    }
    test(name + " w/o index cache, constraining maxBlocksInFlightPerAddress") {
      body(indexCacheDisabledConf.set(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS.key, "1"))
    }
    test(name + " w index cache, constraining maxBlocksInFlightPerAddress") {
      body(indexCacheEnabledConf.set(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS.key, "1"))
    }
    val default = RemoteShuffleConf.DATA_FETCH_EAGER_REQUIREMENT.defaultValue.get
    val testWith = (true ^ default)
    test(name + s" with eager requirement = ${testWith}") {
      body(indexCacheEnabledConf.set(
        RemoteShuffleConf.DATA_FETCH_EAGER_REQUIREMENT.key, testWith.toString))
    }
  }

  private def prepareMapOutput(
      resolver: RemoteShuffleBlockResolver, shuffleId: Int, mapId: Int, blocks: Array[Byte]*) {
    val dataTmp = RemoteShuffleUtils.tempPathWith(resolver.getDataFile(shuffleId, mapId))
    val fs = resolver.fs
    val out = fs.create(dataTmp)
    val lengths = new ArrayBuffer[Long]
    Utils.tryWithSafeFinally {
      for (block <- blocks) {
        lengths += block.length
        out.write(block)
      }
    } {
      out.close()
    }
    // Actually this UT relies on this outside function's fine working
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths.toArray, dataTmp)
  }

  private def basicRead(conf: SparkConf): Unit = {

    sc = new SparkContext("local[1]", "Shuffle Iterator read", conf)
    val shuffleId = 1

    val env = SparkEnv.get
    val resolver = env.shuffleManager.shuffleBlockResolver.asInstanceOf[RemoteShuffleBlockResolver]
    // There are two transferServices, use the one exclusively for RemoteShuffle
    val transferService = env.shuffleManager.shuffleBlockResolver
      .asInstanceOf[RemoteShuffleBlockResolver].remoteShuffleTransferService
    val shuffleServerId =
      transferService.asInstanceOf[RemoteShuffleTransferService].getShuffleServerId

    val numMaps = 3

    val expectPart0 = Array[Byte](1)
    val expectPart1 = Array[Byte](6, 4)
    val expectPart2 = Array[Byte](0, 2)
    val expectPart3 = Array[Byte](28)
    val expectPart4 = Array[Byte](96, 97)
    val expectPart5 = Array[Byte](95)

    prepareMapOutput(
      resolver, shuffleId, 0, Array[Byte](3, 6, 9), expectPart0, expectPart1)
    prepareMapOutput(
      resolver, shuffleId, 1, Array[Byte](19, 94), expectPart2, expectPart3)
    prepareMapOutput(
      resolver, shuffleId, 2, Array[Byte](99, 98), expectPart4, expectPart5)

    val startPartition = 1
    val endPartition = 3

    val blockInfos = for (i <- 0 until numMaps; j <- startPartition until endPartition) yield {
      (ShuffleBlockId(shuffleId, i, j), 1L)
    }

    val blocksByAddress = Seq((shuffleServerId, blockInfos))

    val iter = new RemoteShuffleBlockIterator(
      TaskContext.empty(),
      transferService,
      resolver,
      blocksByAddress.toIterator,
      (_: BlockId, input: InputStream) => input,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      true)

    val expected =
      expectPart0 ++ expectPart1 ++ expectPart2 ++ expectPart3 ++ expectPart4 ++ expectPart5

    val answer = new ArrayBuffer[Byte]()
    iter.map(_._2).foreach { case input =>
      var current: Int = -1
      while ({current = input.read(); current != -1}) {
        answer += current.toByte
      }
    }
    // Shuffle doesn't guarantee that the blocks are returned as ordered in blockInfos,
    // so the answer and expected should be sorted before compared
    assert(answer.map(_.toInt).sorted.zip(expected.map(_.toInt).sorted)
        .forall{case (byteAns, byteExp) => byteAns === byteExp})
  }

  private def cleanAll(files: Path*): Unit = {
    for (file <- files) {
      deleteFileAndTempWithPrefix(file)
    }
  }

  private def deleteFileAndTempWithPrefix(prefixPath: Path): Unit = {
    val fs = prefixPath.getFileSystem(new Configuration(false))
    val parentDir = prefixPath.getParent
    val iter = fs.listFiles(parentDir, false)
    while (iter.hasNext) {
      val file = iter.next()
      if (file.getPath.toString.contains(prefixPath.getName)) {
        fs.delete(file.getPath, true)
      }
    }
  }
}
