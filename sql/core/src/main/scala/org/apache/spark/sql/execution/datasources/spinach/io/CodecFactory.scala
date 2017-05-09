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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.parquet.hadoop.metadata.CompressionCodecName

// This is a simple version of parquet's CodeFactory.
// TODO: [linhong] Need change this into Scala Code style
// TODO: [linhong] Need add release() function
private[spinach] class CodecFactory(conf: Configuration) {

  private def getCodec(codecString: String): Option[CompressionCodec] = {
    val codecName = CompressionCodecName.valueOf(codecString)
    val codecClass = codecName.getHadoopCompressionCodecClass
    if (codecClass == null) None
    else Some(ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec])
  }

  def getCompressor(codec: org.apache.parquet.format.CompressionCodec): BytesCompressor = {
    val compressionCodec = getCodec(codec.name)
    new BytesCompressor(compressionCodec)
  }

  def getDecompressor(codec: org.apache.parquet.format.CompressionCodec): BytesDecompressor = {
    val compressionCodec = getCodec(codec.name)
    new BytesDecompressor(compressionCodec)
  }
}

private[spinach] class BytesCompressor(compressionCodec: Option[CompressionCodec]) {

  private lazy val compressor = CodecPool.getCompressor(compressionCodec.get)
  private lazy val compressedOutBuffer = new ByteArrayOutputStream()

  def compress(bytes: Array[Byte]): Array[Byte] = {
    compressionCodec match {
      case Some(codec) =>
        compressedOutBuffer.reset()
        // null compressor for non-native gzip
        if (compressor != null) compressor.reset()
        val cos = codec.createOutputStream(compressedOutBuffer, compressor)
        cos.write(bytes)
        cos.finish()
        cos.close()
        compressedOutBuffer.toByteArray
      case None => bytes
    }
  }
}

private[spinach] class BytesDecompressor(compressionCodec: Option[CompressionCodec]) {

  private lazy val decompressor = CodecPool.getDecompressor(compressionCodec.get)

  def decompress(bytes: Array[Byte], uncompressedSize: Int): Array[Byte] = {
    compressionCodec match {
      case Some(codec) =>
        decompressor.reset()
        val cis = codec.createInputStream(new ByteArrayInputStream(bytes), decompressor)
        val decompressed = new Array[Byte](uncompressedSize)
        cis.read(decompressed)
        decompressed
      case None => bytes
    }
  }
}
