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

package org.apache.spark.sql.execution.datasources.oap.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.parquet.format.{CompressionCodec => ParquetCodec}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

// This is a simple version of parquet's CodeFactory.
// TODO: [linhong] Need change this into Scala Code style
private[oap] class CodecFactory(conf: Configuration) {

  private val compressors = new mutable.HashMap[ParquetCodec, BytesCompressor]
  private val decompressors = new mutable.HashMap[ParquetCodec, BytesDecompressor]
  private val codecByName = new mutable.HashMap[String, CompressionCodec]

  private def getCodec(codecString: String): Option[CompressionCodec] = {
    codecByName.get(codecString) match {
      case Some(codec) => Some(codec)
      case None =>
        val codecName = CompressionCodecName.valueOf(codecString)
        val codecClass = codecName.getHadoopCompressionCodecClass
        if (codecClass == null) {
          None
        } else {
          val codec = ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec]
          codecByName.put(codecString, codec)
          Some(codec)
        }
    }
  }

  def getCompressor(codec: ParquetCodec): BytesCompressor = {
    compressors.getOrElseUpdate(codec, new BytesCompressor(getCodec(codec.name)))
  }

  def getDecompressor(codec: ParquetCodec): BytesDecompressor = {
    decompressors.getOrElseUpdate(codec, new BytesDecompressor(getCodec(codec.name)))
  }

  def release(): Unit = {
    compressors.values.foreach(_.release())
    compressors.clear()
    decompressors.values.foreach(_.release())
    decompressors.clear()
  }
}

private[oap] class BytesCompressor(compressionCodec: Option[CompressionCodec]) {

  private lazy val compressedOutBuffer = new ByteArrayOutputStream()
  private lazy val compressor = compressionCodec match {
    case Some(codec) => CodecPool.getCompressor(codec)
    case None => null
  }

  def compress(bytes: Array[Byte]): Array[Byte] = {
    compressionCodec match {
      case Some(codec) =>
        compressedOutBuffer.reset()
        // null compressor for non-native gzip
        if (compressor != null) {
          compressor.reset()
        }
        val cos = codec.createOutputStream(compressedOutBuffer, compressor)
        cos.write(bytes)
        cos.finish()
        cos.close()
        compressedOutBuffer.toByteArray
      case None => bytes
    }
  }

  def release(): Unit = CodecPool.returnCompressor(compressor)
}

private[oap] class BytesDecompressor(compressionCodec: Option[CompressionCodec]) {

  private lazy val decompressor = compressionCodec match {
    case Some(codec) => CodecPool.getDecompressor(codec)
    case None => null
  }

  def decompress(bytes: Array[Byte], uncompressedSize: Int): Array[Byte] = {
    compressionCodec match {
      case Some(codec) =>
        decompressor.reset()
        val cis = codec.createInputStream(new ByteArrayInputStream(bytes), decompressor)
        val decompressed = new Array[Byte](uncompressedSize)
        new DataInputStream(cis).readFully(decompressed)
        decompressed
      case None => bytes
    }
  }

  def release(): Unit = CodecPool.returnDecompressor(decompressor)
}
