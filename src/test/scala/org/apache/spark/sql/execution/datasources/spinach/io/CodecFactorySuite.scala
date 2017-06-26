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

package org.apache.spark.sql.execution.datasources.spinach.io

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.format.CompressionCodec
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Prop.forAllNoShrink
import org.scalatest.prop.Checkers

import org.apache.spark.SparkFunSuite


class CodecFactoryCheck extends Properties("CodecFactory") {

  private val codecFactory = new CodecFactory(new Configuration())

  private val gen = Gen.sized { size =>
    for {
      codec <- Arbitrary.arbitrary[CompressionCodec]
      times <- Gen.posNum[Int]
      bytes <- Gen.containerOfN[Array, Byte](size * 100, Arbitrary.arbitrary[Byte])
    } yield (codec, times, bytes)
  }

  property("compress/decompress") = forAllNoShrink(gen) {
    // Array[Array[Byte]] means one group of fibers' data
    case (codec, times, bytes) =>
      val compressor = codecFactory.getCompressor(codec)
      val decompressor = codecFactory.getDecompressor(codec)

      !(0 until times).exists{ _ =>
        !decompressor.decompress(compressor.compress(bytes), bytes.length).sameElements(bytes)
      }
  }

  implicit lazy val arbCompressionCodec: Arbitrary[CompressionCodec] = {
    Arbitrary(genCompressionCodec)
  }
  private lazy val genCompressionCodec: Gen[CompressionCodec] = Gen.oneOf(
    CompressionCodec.UNCOMPRESSED, CompressionCodec.GZIP,
    CompressionCodec.SNAPPY, CompressionCodec.LZO)
}
class CodecFactorySuite extends SparkFunSuite with Checkers {

  test("Check CodecFactory Compress/Decompress") {
    check(new CodecFactoryCheck)
  }
}
