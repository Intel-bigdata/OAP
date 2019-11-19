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

import java.io.InputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.storage.{BlockId, ShuffleBlockId}
import org.apache.spark.util.Utils

class RemoteShuffleBlockIteratorSuite extends SparkFunSuite with LocalSparkContext {

  testWithAndWithoutIndexCache("basic read")(basicRead)

  private def testWithAndWithoutIndexCache(name: String, loadDefaults: Boolean = true)
      (body: (SparkConf => Unit)): Unit = {
    test(name + " without index cache") {
      body(createDefaultConf(loadDefaults))
    }
    test(name + " with index cache") {
      body(createDefaultConfWithIndexCacheEnabled(loadDefaults))
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
    val blockManager = env.blockManager
    val resolver = env.shuffleManager.shuffleBlockResolver.asInstanceOf[RemoteShuffleBlockResolver]
    val transferService = blockManager.blockTransferService

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

    val blocksByAddress = Seq((blockManager.blockManagerId, blockInfos))

    val iter = new RemoteShuffleBlockIterator(
      TaskContext.empty(),
      transferService,
      resolver,
      blocksByAddress.toIterator,
      (_: BlockId, input: InputStream) => input,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      conf)

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
