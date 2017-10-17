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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.File
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import org.roaringbitmap.RoaringBitmap
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.Utils


/**
 * Microbenchmark for BitMap.
 */
class BitMapMicroBenchmark extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._
  private var dir: File = null

  override def beforeEach(): Unit = {
    sqlContext.conf.setConf(SQLConf.OAP_IS_TESTING, true)
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

  test("Microbenchmark for Bitmap index") {
    val intArray: Array[Int] = Array(100000, 10000, 1000, 100, 10, 1)
    val bs = new BitSet(100000)
    val rb = new RoaringBitmap()
    intArray.foreach(bs.set)
    intArray.foreach(rb.add)

    val bsByteArrayBuf = new ByteArrayOutputStream()
    val bsOut = new ObjectOutputStream(bsByteArrayBuf)
    bsOut.writeObject(bs)
    bsOut.flush()
    bsOut.close()

    rb.runOptimize()
    val rbSeBytes = rb.serializedSizeInBytes()
    val rbByteArrayBuf = new ByteArrayOutputStream()
    val rbOut = new ObjectOutputStream(rbByteArrayBuf)
    rbOut.writeObject(rb)
    rbOut.close()

    val rbBuf2 = new ByteArrayOutputStream()
    val rbOut2 = new ObjectOutputStream(rbBuf2)
    rb.writeExternal(rbOut2)
    rbOut2.close()

    // scalastyle:off println
    println("BitSet bitmap size(B) " + bsByteArrayBuf.size) // value is 12656
    println("RoaringBitmap size(B) " + rbByteArrayBuf.size) // value is 91
    println("RoaringBitmap serialize size(B) " + rbSeBytes) // value is 36
    println("RoaringBitmap writeExternal size(B) " + rbBuf2.size) // value is 42. 12656/42 = 301.
    // scalastyle:on println
  }

  /*                             record numbers    30000    300000              3000000
   *         bitmap index file size with BitSet    866772   (2.84GB)2839781768  OOM
   * bitmap index file size with roaring bitmap    162208   (7.48MB)7481772     73MB(73631772)
   *                                      ratio    5.34     379.56              oo
   */
  // TODO: Tuning the roaring bitmap usage to further reduce bitmap index file size.
  test("test the bitmap index file size with BitSet and Roaring Bitmap") {
    val data: Seq[(Int, String)] = (1 to 30000).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    sql("create oindex index_bm on oap_test (a) USING BITMAP")
    val fileNameIterator = dir.listFiles()
    for (fileName <- fileNameIterator) {
      if (fileName.toString().endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
      // scalastyle:off println
      println("bitmap index file size " + fileName.length)
      // scalastyle:on println
      }
    }
    sql("drop oindex index_bm on oap_test")
  }

}
