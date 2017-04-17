/**
  * Created by linhongl on 07/04/2017.
  */

package org.apache.spark.sql.execution.datasources.spinach.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.parquet.hadoop.metadata.CompressionCodecName


// This is a simple version of parquet's CodeFacotry.
private[spinach] class CodecFactory(conf: Configuration) {

  def getCodec(codecString: String): CompressionCodec = {
    val codecName = CompressionCodecName.valueOf(codecString)
    val codecClass = codecName.getHadoopCompressionCodecClass
    if (codecClass == null) null
    else ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec]
  }

  def getCompressor(codecString: String): BytesCompressor = {
    val codec = getCodec(codecString)
    new BytesCompressor(codec)
  }

  def getDecompressor(codecString: String): BytesDecompressor = {
    val codec = getCodec(codecString)
    new BytesDecompressor(codec)
  }
}

private[spinach] class BytesCompressor(codec: CompressionCodec) {

  val compressor = if (codec == null) null else CodecPool.getCompressor(codec)

  def compress(bytes: Array[Byte]): Array[Byte] = {
    if (codec == null) {
      bytes
    } else {
      val compressedOutBuffer = new ByteArrayOutputStream()
      val cos = codec.createOutputStream(compressedOutBuffer, compressor)
      cos.write(bytes)
      cos.finish()
      cos.close()
      compressedOutBuffer.toByteArray
    }
  }
}

private[spinach] class BytesDecompressor(codec: CompressionCodec) {

  val decompressor = if (codec == null) null else CodecPool.getDecompressor(codec)

  def decompress(bytes: Array[Byte], uncompressedSize: Int): Array[Byte] = {
    if (codec == null) {
      bytes
    } else {
      decompressor.reset()
      val cis = codec.createInputStream(new ByteArrayInputStream(bytes), decompressor)
      val decompressed = new Array[Byte](uncompressedSize)
      cis.read(decompressed)
      decompressed
    }
  }
}