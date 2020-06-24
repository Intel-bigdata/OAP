package org.apache.spark.storage.pmof

import java.util.concurrent.atomic.AtomicLong
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, UnpooledByteBufAllocator}
import scala.collection.mutable.Stack
import java.lang.RuntimeException
import org.apache.spark.internal.Logging
import scala.collection.mutable.Map
import java.nio.ByteBuffer

object NettyByteBufferPool extends Logging {
  private val allocatedBufRenCnt: AtomicLong = new AtomicLong(0)
  private val allocatedBytes: AtomicLong = new AtomicLong(0)
  private val peakAllocatedBytes: AtomicLong = new AtomicLong(0)
  private val unpooledAllocatedBytes: AtomicLong = new AtomicLong(0)
  private val bufferMap: Map[ByteBuf, Long] = Map()
  private val allocatedBufferPool: Stack[ByteBuf] = Stack[ByteBuf]()
  private var reachRead = false
  private val allocator = UnpooledByteBufAllocator.DEFAULT

  def allocateNewBuffer(bufSize: Int): ByteBuf = synchronized {
    allocatedBufRenCnt.getAndIncrement()
    allocatedBytes.getAndAdd(bufSize)
    if (allocatedBytes.get > peakAllocatedBytes.get) {
      peakAllocatedBytes.set(allocatedBytes.get)
    }
    try {
      /*if (allocatedBufferPool.isEmpty == false) {
        allocatedBufferPool.pop
      } else {
        allocator.directBuffer(bufSize, bufSize)
      }*/
      val byteBuf = allocator.directBuffer(bufSize, bufSize)
      bufferMap += (byteBuf -> bufSize)
      byteBuf
    } catch {
      case e: Throwable =>
        logError(s"allocateNewBuffer size is ${bufSize}")
        throw e
    }
  }

  def allocateFlexibleNewBuffer(bufSize: Int): ByteBuf = synchronized {
    val byteBuf = allocator.directBuffer(65536, bufSize * 2)
    bufferMap += (byteBuf -> bufSize)
    byteBuf
  }

  def releaseBuffer(buf: ByteBuf): Unit = synchronized {
    allocatedBufRenCnt.getAndDecrement()
    try {
      val bufSize = bufferMap(buf)
      allocatedBytes.getAndAdd(bufSize)

    } catch {
      case e: NoSuchElementException => {}
    }
    buf.clear()
    //allocatedBufferPool.push(buf)
    buf.release(buf.refCnt())
  }

  def unpooledInc(bufSize: Int): Unit = synchronized {
    if (reachRead == false) {
      reachRead = true
      peakAllocatedBytes.set(0)
    }
    unpooledAllocatedBytes.getAndAdd(bufSize)
  }

  def unpooledDec(bufSize: Int): Unit = synchronized {
    unpooledAllocatedBytes.getAndAdd(0 - bufSize)
  }

  def unpooledInc(bufSize: Long): Unit = synchronized {
    if (reachRead == false) {
      reachRead = true
      peakAllocatedBytes.set(0)
    }
    unpooledAllocatedBytes.getAndAdd(bufSize)
  }

  def unpooledDec(bufSize: Long): Unit = synchronized {
    unpooledAllocatedBytes.getAndAdd(0 - bufSize)
  }

  override def toString(): String = synchronized {
    return s"NettyBufferPool [refCnt|allocatedBytes|Peak|Native] is [${allocatedBufRenCnt.get}|${allocatedBytes.get}|${peakAllocatedBytes.get}|${unpooledAllocatedBytes.get}]"
  }

  def dump(byteBuffer: ByteBuffer, size: Int = -1): String = {
    byteBuffer.rewind
    val bufSize = if (size == -1) {
      byteBuffer.remaining()
    } else {
      size
    }
    val tmp = Array.ofDim[Byte](bufSize)
    byteBuffer.get(tmp, 0, bufSize)
    s"${byteBuffer} [${convertBytesToHex(tmp.slice(0, 50))} ... ${convertBytesToHex(
      tmp.slice(bufSize - 51, bufSize - 1))}]"
  }

  def convertBytesToHex(bytes: Seq[Byte]): String = {
    val sb = new StringBuilder
    var len = 0
    for (b <- bytes) {
      sb.append(String.format("%02x ", Byte.box(b)))
      len += 1
      if (len > 128) {
        len = 0
        sb.append("\n")
      }
    }
    sb.toString
  }
}
