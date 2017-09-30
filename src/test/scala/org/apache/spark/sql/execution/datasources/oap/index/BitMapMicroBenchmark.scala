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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import org.roaringbitmap.RoaringBitmap
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.collection.BitSet


/**
 * Microbenchmark for BitMap Index
 */
class BitMapMicroBenchmark extends QueryTest with SharedSQLContext {
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
}
