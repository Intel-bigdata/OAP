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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.Utils

class RemoteShuffleBlockResolverSuite extends SparkFunSuite with BeforeAndAfterEach {

  val conf = createDefaultConf()

  var dataFile: Path = _
  var indexFile: Path = _
  var dataTmp: Path = _
  var shuffleManager: RemoteShuffleManager = _
  val shuffleId = 1
  val mapId = 2

  test("Commit shuffle files multiple times") {

    shuffleManager = new RemoteShuffleManager(conf)
    val resolver = shuffleManager.shuffleBlockResolver

    indexFile = resolver.getIndexFile(shuffleId, mapId)
    dataFile = resolver.getDataFile(shuffleId, mapId)
    val fs = resolver.fs

    dataTmp = RemoteShuffleUtils.tempPathWith(dataFile)

    val lengths = Array[Long](10, 0, 20)
    val out = fs.create(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    assert(fs.exists(indexFile))
    assert(fs.getFileStatus(indexFile).getLen() === (lengths.length + 1) * 8)
    assert(fs.exists(dataFile))
    assert(fs.getFileStatus(dataFile).getLen() === 30)
    assert(!fs.exists(dataTmp))

    val lengths2 = new Array[Long](3)
    val dataTmp2 = RemoteShuffleUtils.tempPathWith(dataFile)
    val out2 = fs.create(dataTmp2)
    Utils.tryWithSafeFinally {
      out2.write(Array[Byte](1))
      out2.write(new Array[Byte](29))
    } {
      out2.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths2, dataTmp2)

    assert(fs.getFileStatus(indexFile).getLen() === (lengths.length + 1) * 8)
    assert(lengths2.toSeq === lengths.toSeq)
    assert(fs.exists(dataFile))
    assert(fs.getFileStatus(dataFile).getLen() === 30)
    assert(!fs.exists(dataTmp2))

    // The dataFile should be the previous one
    val firstByte = new Array[Byte](1)
    val dataIn = fs.open(dataFile)
    Utils.tryWithSafeFinally {
      dataIn.read(firstByte)
    } {
      dataIn.close()
    }
    assert(firstByte(0) === 0)

    // The index file should not change
    val indexIn = fs.open(indexFile)
    Utils.tryWithSafeFinally {
      indexIn.readLong() // the first offset is always 0
      assert(indexIn.readLong() === 10, "The index file should not change")
    } {
      indexIn.close()
    }

    // remove data file
    fs.delete(dataFile, true)

    val lengths3 = Array[Long](7, 10, 15, 3)
    val dataTmp3 = RemoteShuffleUtils.tempPathWith(dataFile)
    val out3 = fs.create(dataTmp3)
    Utils.tryWithSafeFinally {
      out3.write(Array[Byte](2))
      out3.write(new Array[Byte](34))
    } {
      out3.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths3, dataTmp3)
    assert(fs.getFileStatus(indexFile).getLen() === (lengths3.length + 1) * 8)
    assert(lengths3.toSeq != lengths.toSeq)
    assert(fs.exists(dataFile))
    assert(fs.getFileStatus(dataFile).getLen() === 35)
    assert(!fs.exists(dataTmp3))

    // The dataFile should be the new one, since we deleted the dataFile from the first attempt
    val dataIn2 = fs.open(dataFile)
    Utils.tryWithSafeFinally {
      dataIn2.read(firstByte)
    } {
      dataIn2.close()
    }
    assert(firstByte(0) === 2)

    // The index file should be updated, since we deleted the dataFile from the first attempt
    val indexIn2 = fs.open(indexFile)
    Utils.tryWithSafeFinally {
      indexIn2.readLong() // the first offset is always 0
      assert(indexIn2.readLong() === 7, "The index file should be updated")
    } {
      indexIn2.close()
    }
  }

  test("get block data") {

    shuffleManager = new RemoteShuffleManager(conf)
    val resolver = shuffleManager.shuffleBlockResolver

    indexFile = resolver.getIndexFile(shuffleId, mapId)
    dataFile = resolver.getDataFile(shuffleId, mapId)
    val fs = resolver.fs

    val partitionId = 3
    val expected = Array[Byte](8, 7, 6, 5)
    val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, partitionId)

    val lengths = Array[Long](3, 1, 2, 4)
    dataTmp = RemoteShuffleUtils.tempPathWith(dataFile)
    val out = fs.create(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(Array[Byte](3, 6, 9))
      out.write(Array[Byte](1))
      out.write(Array[Byte](2, 4))
      out.write(expected)
    } {
      out.close()
    }
    // Actually this UT relies on this outside function's fine working
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val answerBuffer =
      resolver.getBlockData(shuffleBlockId).asInstanceOf[HadoopFileSegmentManagedBuffer]
    val expectedBuffer = new HadoopFileSegmentManagedBuffer(dataFile, 6, expected.length)
    assert(expectedBuffer.equals(answerBuffer))
  }

  // With Adaptive Execution enabled, may require a ShuffleBlockBatchId
  test("get block data(Batch)") {

    shuffleManager = new RemoteShuffleManager(conf)
    val resolver = shuffleManager.shuffleBlockResolver

    indexFile = resolver.getIndexFile(shuffleId, mapId)
    dataFile = resolver.getDataFile(shuffleId, mapId)
    val fs = resolver.fs

    val (partitionStartId, partitionEndId) = (1, 4)
    val expected = Array(Array[Byte](99), Array[Byte](2, 4), Array[Byte](8, 7, 6, 5))
    val shuffleBlockId = ShuffleBlockBatchId(shuffleId, mapId, partitionStartId, partitionEndId)

    val lengths = Array[Long](3, 1, 2, 4)
    dataTmp = RemoteShuffleUtils.tempPathWith(dataFile)
    val out = fs.create(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(Array[Byte](3, 6, 9))
      for (each <- expected) {
        out.write(each)
      }
    } {
      out.close()
    }
    // Actually this UT relies on this outside function's fine working
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val answerBuffer =
      resolver.getBlockData(shuffleBlockId).asInstanceOf[HadoopFileSegmentManagedBuffer]
    val expectedBuffer = new HadoopFileSegmentManagedBuffer(dataFile, 3, expected.map(_.length).sum)
    assert(expectedBuffer.equals(answerBuffer))
  }

  test("createInputStream of HadoopFileSegmentManagedBuffer") {

    shuffleManager = new RemoteShuffleManager(conf)
    val resolver = shuffleManager.shuffleBlockResolver

    dataFile = resolver.getDataFile(shuffleId, mapId)
    val fs = resolver.fs

    val out = fs.create(dataFile)
    val expected = Array[Byte](2, 4)
    Utils.tryWithSafeFinally {
      out.write(Array[Byte](3, 6, 9))
      out.write(Array[Byte](1))
      out.write(expected)
      out.write(Array[Byte](8, 7, 6, 5))
    } {
      out.close()
    }

    val answer = new Array[Byte](2)
    val buf = new HadoopFileSegmentManagedBuffer(dataFile, 4, 2)
    val inputStream = buf.createInputStream()
    inputStream.read(answer)
    assert(expected === answer)
    assert(inputStream.available() == 0)
  }

  test("createInputStream of HadoopFileSegmentManagedBuffer, with no data") {

    shuffleManager = new RemoteShuffleManager(conf)
    val resolver = shuffleManager.shuffleBlockResolver

    dataFile = resolver.getDataFile(shuffleId, mapId)
    val fs = resolver.fs

    val answer = new Array[Byte](0)
    val expected = new Array[Byte](0)
    val buf = new HadoopFileSegmentManagedBuffer(dataFile, 4, 0)
    val inputStream = buf.createInputStream()
    inputStream.read(answer)
    assert(expected === answer)
    assert(inputStream.available() == 0)
  }

  private def deleteFilesWithPrefix(prefixPath: Path): Unit = {
    val fs = prefixPath.getFileSystem(new Configuration(false))
    val parentDir = prefixPath.getParent
    if (fs.exists(parentDir)) {
      val iter = fs.listFiles(parentDir, false)
      while (iter.hasNext) {
        val file = iter.next()
        if (file.getPath.toString.contains(prefixPath.getName)) {
          fs.delete(file.getPath, true)
        }
      }
    }
  }

  override def afterEach() {
    super.afterEach()
    if (dataFile != null) {
      // Also delete tmp files if needed
      deleteFilesWithPrefix(dataFile)
    }

    if (indexFile != null) {
      // Also delete tmp files if needed
      deleteFilesWithPrefix(indexFile)
    }
    if (shuffleManager != null) {
      shuffleManager.stop()
    }
  }

}
