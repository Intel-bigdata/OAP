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

package org.apache.spark.sql.execution.datasources.oap.utils

import java.io.{ByteArrayOutputStream, DataOutputStream, File, FileOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{BitmapFiber, MemoryManager, FiberCache, FiberCacheManager}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

// Below are used to test the functionality of OapBitmapWrappedFiberCache class.
class OapBitmapWrappedFiberCacheSuite
  extends QueryTest with SharedOapContext {

  private def loadRbFile(fin: FSDataInputStream, offset: Long, size: Int): FiberCache =
    MemoryManager.toIndexFiberCache(fin, offset, size)

  test("test the functionality of OapBitmapWrappedFiberCache class") {
    val CHUNK_SIZE = 1 << 16
    val dataForRunChunk = (1 to 9).toSeq
    val dataForArrayChunk = Seq(1, 3, 5, 7, 9)
    val dataForBitmapChunk = (1 to 10000).filter(_ % 2 == 1)
    val dataCombination =
      dataForBitmapChunk ++ dataForArrayChunk ++ dataForRunChunk
    val dataArray =
      Array(dataForRunChunk, dataForArrayChunk, dataForBitmapChunk, dataCombination)
    dataArray.foreach(dataIdx => {
      val dir = Utils.createTempDir()
      val rb = new RoaringBitmap()
      dataIdx.foreach(rb.add)
      val rbFile = dir.getAbsolutePath + "rb.bin"
      rb.runOptimize()
      val rbFos = new FileOutputStream(rbFile)
      val rbBos = new ByteArrayOutputStream()
      val rbDos = new DataOutputStream(rbBos)
      rb.serialize(rbDos)
      rbBos.writeTo(rbFos)
      rbBos.close()
      rbDos.close()
      rbFos.close()
      val rbPath = new Path(rbFile.toString)
      val conf = new Configuration()
      val fin = rbPath.getFileSystem(conf).open(rbPath)
      val rbFileSize = rbPath.getFileSystem(conf).getFileStatus(rbPath).getLen
      val rbFiber = BitmapFiber(() => loadRbFile(fin, 0L, rbFileSize.toInt), rbPath.toString, 0, 0)
      val rbWfc = new OapBitmapWrappedFiberCache(FiberCacheManager.get(rbFiber, conf))
      rbWfc.init
      val chunkLength = rbWfc.getTotalChunkLength
      val length = dataIdx.size / CHUNK_SIZE
      assert(chunkLength == (length + 1))
      val chunkKeys = rbWfc.getChunkKeys
      assert(chunkKeys(0).toInt == 0)
      rbWfc.setOffset(0)
      val chunk = rbWfc.getIteratorForChunk(0)
      chunk match {
        case RunChunkIterator(rbWfc) => assert(chunk == RunChunkIterator(rbWfc))
        case ArrayChunkIterator(rbWfc, 0) => assert(chunk == ArrayChunkIterator(rbWfc, 0))
        case BitmapChunkIterator(rbWfc) => assert(chunk == BitmapChunkIterator(rbWfc))
        case _ => throw new OapException("unexpected chunk in OapBitmapWrappedFiberCache.")
      }
      rbWfc.release
      fin.close
      dir.delete
    })
  }
}
