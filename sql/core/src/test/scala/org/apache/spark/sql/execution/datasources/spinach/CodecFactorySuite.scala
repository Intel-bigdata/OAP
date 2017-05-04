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

package org.apache.spark.sql.execution.datasources.spinach

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.format.CompressionCodec
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalatest.prop.Checkers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.spinach.io.CodecFactory

class CodecFactoryCheck extends Properties("CodecFactory") {

  private val codecFactory = new CodecFactory(new Configuration())

  property("compress/decompress") = forAll {
    // Array[Array[Byte]] means one group of fibers' data
    (codec: CompressionCodec, bytesArray: Array[Array[Byte]]) =>
      val compressor = codecFactory.getCompressor(codec)
      val decompressor = codecFactory.getDecompressor(codec)

      !bytesArray.exists(bytes =>
        decompressor.decompress(compressor.compress(bytes), bytes.length)
        .zip(bytes).exists(b => !(b._1 equals b._2))
      )
  }
}
class CodecFactorySuite extends SparkFunSuite with Checkers {

  test("Check CodecFacoty Compress/Decompress") {
    check(new CodecFactoryCheck)
  }
}
