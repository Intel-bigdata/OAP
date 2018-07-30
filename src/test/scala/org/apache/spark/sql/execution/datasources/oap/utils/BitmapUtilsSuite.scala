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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.execution.datasources.oap.filecache.{BitmapFiberId, FiberCache}
import org.apache.spark.sql.execution.datasources.oap.index.{BitmapIndexSectionId, IndexUtils}
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class BitmapUtilsSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  import testImplicits._
  private val BITMAP_FOOTER_SIZE = 6 * 8
  private var dir: File = _

// Below data are used to test the functionality of directly getting
// row IDs from fiber caches with roarting bitmap bypassed.
  private val dataForRunChunk: Seq[(Int, String)] =
    (1 to 20000).map{i => (i / 100, s"this is test $i")}
  private val dataForArrayChunk: Seq[(Int, String)] =
    (1 to 20000).map{i => (i, s"this is test $i")}
  private val dataForBitmapChunk: Seq[(Int, String)] =
    (1 to 20000).map{i => (i % 2, s"this is test $i")}
  private val expectedRowIdSeq = (0 until 20000).toSeq
  private val expectedRowIdSeqCombinationTotal = (0 until 60000).toSeq
  private val dataCombinationTotal =
    dataForBitmapChunk ++ dataForArrayChunk ++ dataForRunChunk
  private val dataSourceArray =
    Array((dataForRunChunk, expectedRowIdSeq), (dataForArrayChunk, expectedRowIdSeq),
      (dataForBitmapChunk, expectedRowIdSeq),
      (dataCombinationTotal, expectedRowIdSeqCombinationTotal))
  private lazy val fiberCacheManager = OapRuntime.getOrCreate.fiberCacheManager

  override def beforeEach(): Unit = {
    dir = Utils.createTempDir()
    val path = dir.getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
            | USING oap
            | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
    dir.delete()
  }

  private def loadBmSection(fin: FSDataInputStream, offset: Long, size: Int): FiberCache =
    OapRuntime.getOrCreate.memoryManager.toIndexFiberCache(fin, offset, size)

  private def getIdxOffset(fiberCache: FiberCache, baseOffset: Long, idx: Int): Int = {
    val idxOffset = baseOffset + idx * 4
    fiberCache.getInt(idxOffset)
  }

  private def getMetaDataAndFiberCaches(
      fin: FSDataInputStream,
      idxPath: Path,
      conf: Configuration): (Int, FiberCache, FiberCache) = {
    val idxFileSize = idxPath.getFileSystem(conf).getFileStatus(idxPath).getLen
    val footerOffset = idxFileSize - BITMAP_FOOTER_SIZE
    val footerFiber = BitmapFiberId(
      () => loadBmSection(fin, footerOffset, BITMAP_FOOTER_SIZE),
      idxPath.toString, BitmapIndexSectionId.footerSection, 0)
    val footerCache = fiberCacheManager.get(footerFiber)
    val uniqueKeyListTotalSize = footerCache.getInt(IndexUtils.INT_SIZE)
    val keyCount = footerCache.getInt(IndexUtils.INT_SIZE * 2)
    val entryListTotalSize = footerCache.getInt(IndexUtils.INT_SIZE * 3)
    val offsetListTotalSize = footerCache.getInt(IndexUtils.INT_SIZE * 4)
    val nullEntrySize = footerCache.getInt(IndexUtils.INT_SIZE * 6)
    val entryListOffset = IndexFile.VERSION_LENGTH + uniqueKeyListTotalSize
    val offsetListOffset = entryListOffset + entryListTotalSize + nullEntrySize
    val offsetListFiber = BitmapFiberId(
      () => loadBmSection(fin, offsetListOffset.toLong, offsetListTotalSize),
      idxPath.toString, BitmapIndexSectionId.entryOffsetsSection, 0)
    val offsetListCache = fiberCacheManager.get(offsetListFiber)
    (keyCount, offsetListCache, footerCache)
  }

  test("test how to directly get the row ID list from single fiber cache without roaring bitmap") {
    dataSourceArray.foreach(dataSourceElement => {
      dataSourceElement._1.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table oap_test select * from t")
      sql("create oindex index_bm on oap_test (a) USING BITMAP")
      var actualRowIdSeq = Seq.empty[Int]
      var accumulatorRowId = 0
      dir.listFiles.foreach(fileName => {
        if (fileName.toString.endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
          var maxRowIdInPartition = 0
          val idxPath = new Path(fileName.toString)
          val conf = new Configuration()
          val fin = idxPath.getFileSystem(conf).open(idxPath)
          val (keyCount, offsetListCache, footerCache) =
            getMetaDataAndFiberCaches(fin, idxPath, conf)
          (0 until keyCount).map(idx => {
            val curIdxOffset = getIdxOffset(offsetListCache, 0L, idx)
            val entrySize = getIdxOffset(offsetListCache, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiberId(
              () => loadBmSection(fin, curIdxOffset.toLong, entrySize), idxPath.toString,
            BitmapIndexSectionId.entryListSection, idx)
            val wrappedFiberCacheSeq =
              Seq(new OapBitmapWrappedFiberCache(fiberCacheManager.get(entryFiber)))
            BitmapUtils.iterator(wrappedFiberCacheSeq).foreach(rowId => {
              actualRowIdSeq :+= rowId + accumulatorRowId
              if (maxRowIdInPartition < rowId) maxRowIdInPartition = rowId
            })
            wrappedFiberCacheSeq.head.release
          })
          // The row Id is starting from 0.
          accumulatorRowId += maxRowIdInPartition + 1
          footerCache.release
          offsetListCache.release
          fin.close
        }
      })
      actualRowIdSeq.foreach(rowId => assert(dataSourceElement._2.contains(rowId)))
      dataSourceElement._2.foreach(rowId => assert(actualRowIdSeq.contains(rowId)))
      sql("drop oindex index_bm on oap_test")
    })
  }

  test("test how to directly get the row Id list after bitwise OR among multi fiber caches") {
    dataSourceArray.foreach(dataSourceElement => {
      dataSourceElement._1.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table oap_test select * from t")
      sql("create oindex index_bm on oap_test (a) USING BITMAP")
      var actualRowIdSeq = Seq.empty[Int]
      var accumulatorRowId = 0
      dir.listFiles.foreach(fileName => {
        if (fileName.toString.endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
          var maxRowIdInPartition = 0
          val idxPath = new Path(fileName.toString)
          val conf = new Configuration()
          val fin = idxPath.getFileSystem(conf).open(idxPath)
          val (keyCount, offsetListCache, footerCache) =
            getMetaDataAndFiberCaches(fin, idxPath, conf)
          val wrappedFiberCacheSeq = (0 until keyCount).map(idx => {
            val curIdxOffset = getIdxOffset(offsetListCache, 0L, idx)
            val entrySize = getIdxOffset(offsetListCache, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiberId(
              () => loadBmSection(fin, curIdxOffset.toLong, entrySize), idxPath.toString,
              BitmapIndexSectionId.entryListSection, idx)
            new OapBitmapWrappedFiberCache(fiberCacheManager.get(entryFiber))
          })
          BitmapUtils.iterator(wrappedFiberCacheSeq).foreach(rowId => {
            actualRowIdSeq :+= rowId + accumulatorRowId
            if (maxRowIdInPartition < rowId) maxRowIdInPartition = rowId
          })
          accumulatorRowId += maxRowIdInPartition + 1
          wrappedFiberCacheSeq.foreach(wfc => wfc.release)
          footerCache.release
          offsetListCache.release
          fin.close
        }
      })
      actualRowIdSeq.foreach(rowId => assert(dataSourceElement._2.contains(rowId)))
      dataSourceElement._2.foreach(rowId => assert(actualRowIdSeq.contains(rowId)))
      sql("drop oindex index_bm on oap_test")
    })
  }
}
