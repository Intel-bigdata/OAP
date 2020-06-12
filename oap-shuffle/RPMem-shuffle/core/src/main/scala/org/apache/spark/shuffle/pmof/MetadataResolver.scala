package org.apache.spark.shuffle.pmof

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import java.util.zip.{Deflater, DeflaterOutputStream, Inflater, InflaterInputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInputStream, ByteBufferOutputStream, Input, Output}
import org.apache.spark.SparkEnv
import org.apache.spark.network.pmof._
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{ShuffleBlockId, ShuffleDataBlockId}
import org.apache.spark.util.configuration.pmof.PmofConf

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.{Breaks, ControlThrowable}

/**
  * This class is to handle shuffle block metadata
  * It can be used by driver to store or lookup metadata
  * and can be used by executor to send metadata to driver
  * @param pmofConf
  */
class MetadataResolver(pmofConf: PmofConf) {
  private[this] val blockManager = SparkEnv.get.blockManager
  private[this] val blockMap: ConcurrentHashMap[String, ShuffleBuffer] = new ConcurrentHashMap[String, ShuffleBuffer]()
  private[this] val blockInfoMap = mutable.HashMap.empty[String, ArrayBuffer[ShuffleBlockInfo]]
  private[this] val info_serialize_stream = new Kryo()
  private[this] val shuffleBlockInfoSerializer = new ShuffleBlockInfoSerializer

  info_serialize_stream.register(classOf[ShuffleBlockInfo], shuffleBlockInfoSerializer)

  /**
    * called by executor, send shuffle block metadata to driver when using persistent memory as shuffle device
    * @param shuffleId
    * @param mapId
    * @param dataAddressMap
    * @param rkey
    */
  def pushPmemBlockInfo(shuffleId: Int, mapId: Int, dataAddressMap: mutable.HashMap[Int, Array[(Long, Int)]], rkey: Long): Unit = {
    val buffer: Array[Byte] = new Array[Byte](pmofConf.reduce_serializer_buffer_size.toInt)
    var output = new Output(buffer)
    val bufferArray = new ArrayBuffer[ByteBuffer]()

    MetadataResolver.this.synchronized {
      for (iterator <- dataAddressMap) {
        for ((address, length) <- iterator._2) {
          val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, iterator._1).name
          info_serialize_stream.writeObject(output, new ShuffleBlockInfo(shuffleBlockId, address, length.toInt, rkey))
          output.flush()
          if (output.position() >= pmofConf.map_serializer_buffer_size * 0.9) {
            val blockBuffer = ByteBuffer.wrap(output.getBuffer)
            blockBuffer.position(output.position())
            blockBuffer.flip()
            bufferArray += blockBuffer
            output.close()
            val new_buffer = new Array[Byte](pmofConf.reduce_serializer_buffer_size.toInt)
            output = new Output(new_buffer)
          }
        }
      }
    }
    if (output.position() != 0) {
      val blockBuffer = ByteBuffer.wrap(output.getBuffer)
      blockBuffer.position(output.position())
      blockBuffer.flip()
      bufferArray += blockBuffer
      output.close()
    }
    val latch = new CountDownLatch(bufferArray.size)
    val receivedCallback = new ReceivedCallback {
      override def onSuccess(obj: ArrayBuffer[ShuffleBlockInfo]): Unit = {
        latch.countDown()
      }

      override def onFailure(e: Throwable): Unit = {

      }
    }
    for (buffer <- bufferArray) {
      PmofTransferService.getTransferServiceInstance(null, null).
        pushBlockInfo(pmofConf.driverHost, pmofConf.driverPort, buffer, 0.toByte, receivedCallback)
    }
    latch.await()
  }

  /**
    * called by executor, send shuffle block metadata to driver when not using persistent memory as shuffle device
    * @param shuffleId
    * @param mapId
    * @param partitionLengths
    */
  def pushFileBlockInfo(shuffleId: Int, mapId: Int, partitionLengths: Array[Long]): Unit = {
    var offset: Long = 0L
    val file = blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
    val channel: FileChannel = new RandomAccessFile(file, "rw").getChannel

    var totalLength = 0L
    for (executorId <- partitionLengths.indices) {
      val currentLength: Long = partitionLengths(executorId)
      totalLength = totalLength + currentLength
    }

    val eqService = PmofTransferService.getTransferServiceInstance(pmofConf, blockManager).server.getEqService
    val shuffleBuffer = new ShuffleBuffer(0, totalLength, channel, eqService)
    val startedAddress = shuffleBuffer.getAddress
    val rdmaBuffer = eqService.regRmaBufferByAddress(shuffleBuffer.nioByteBuffer(), startedAddress, totalLength.toInt)
    shuffleBuffer.setRdmaBufferId(rdmaBuffer.getBufferId)
    shuffleBuffer.setRkey(rdmaBuffer.getRKey)
    val blockId = ShuffleBlockId(shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
    blockMap.put(blockId.name, shuffleBuffer)

    val byteBuffer = ByteBuffer.allocate(pmofConf.map_serializer_buffer_size.toInt)
    val bos = new ByteBufferOutputStream(byteBuffer)
    var output: Output = null
    if (pmofConf.metadataCompress) {
      val dos = new DeflaterOutputStream(bos, new Deflater(9, true))
      output = new Output(dos)
    } else {
      output = new Output(bos)
    }
    MetadataResolver.this.synchronized {
      for (executorId <- partitionLengths.indices) {
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, executorId)
        val currentLength: Int = partitionLengths(executorId).toInt
        val blockNums = currentLength / pmofConf.shuffleBlockSize + (if (currentLength % pmofConf.shuffleBlockSize == 0) 0 else 1)
        for (i <- 0 until blockNums) {
          if (i != blockNums - 1) {
            info_serialize_stream.writeObject(output, new ShuffleBlockInfo(shuffleBlockId.name, startedAddress + offset, pmofConf.shuffleBlockSize, rdmaBuffer.getRKey))
            offset += pmofConf.shuffleBlockSize
          } else {
            info_serialize_stream.writeObject(output, new ShuffleBlockInfo(shuffleBlockId.name, startedAddress + offset, currentLength - (i * pmofConf.shuffleBlockSize), rdmaBuffer.getRKey))
            offset += (currentLength - (i * pmofConf.shuffleBlockSize))
          }
        }
      }
    }

    output.flush()
    output.close()
    byteBuffer.flip()

    val latch = new CountDownLatch(1)

    val receivedCallback = new ReceivedCallback {
      override def onSuccess(obj: ArrayBuffer[ShuffleBlockInfo]): Unit = {
        latch.countDown()
      }

      override def onFailure(e: Throwable): Unit = {

      }
    }

    PmofTransferService.getTransferServiceInstance(null, null).
      pushBlockInfo(pmofConf.driverHost, pmofConf.driverPort, byteBuffer, 0.toByte, receivedCallback)
    latch.await()
  }

  /**
    * called by executor, fetch shuffle block metadata from driver
    * @param blockIds
    * @param receivedCallback
    */
  def fetchBlockInfo(blockIds: Array[ShuffleBlockId], receivedCallback: ReceivedCallback): Unit = {
    val nums = blockIds.length
    val byteBufferTmp = ByteBuffer.allocate(4 + 12 * nums)
    byteBufferTmp.putInt(nums)
    for (i <- 0 until nums) {
      byteBufferTmp.putInt(blockIds(i).shuffleId)
      byteBufferTmp.putInt(blockIds(i).mapId)
      byteBufferTmp.putInt(blockIds(i).reduceId)
    }
    byteBufferTmp.flip()
    PmofTransferService.getTransferServiceInstance(null, null).
      pushBlockInfo(pmofConf.driverHost, pmofConf.driverPort, byteBufferTmp, 1.toByte, receivedCallback)
  }

  /**
    * called by driver, save shuffle block metadata to memory
    * @param byteBuffer
    */
  def saveShuffleBlockInfo(byteBuffer: ByteBuffer): Unit = {
    val bis = new ByteBufferInputStream(byteBuffer)
    var input: Input = null
    if (pmofConf.metadataCompress) {
      val iis = new InflaterInputStream(bis, new Inflater(true))
      input = new Input(iis)
    } else {
      input = new Input(bis)
    }
    MetadataResolver.this.synchronized {
      do {
        if (input.available() > 0) {
          val shuffleBlockInfo = info_serialize_stream.readObject(input, classOf[ShuffleBlockInfo])
          if (blockInfoMap.contains(shuffleBlockInfo.getShuffleBlockId)) {
            blockInfoMap(shuffleBlockInfo.getShuffleBlockId).append(shuffleBlockInfo)
          } else {
            val blockInfoArray = new ArrayBuffer[ShuffleBlockInfo]()
            blockInfoArray.append(shuffleBlockInfo)
            blockInfoMap.put(shuffleBlockInfo.getShuffleBlockId, blockInfoArray)
          }
        } else {
          input.close()
          return
        }
      } while (true)
    }
  }

  /**
    * called by driver, serialize shuffle block metadata object to bytebuffer, then send to executor through RDMA network
    * @param byteBuffer
    * @return
    */
  def serializeShuffleBlockInfo(byteBuffer: ByteBuffer): ArrayBuffer[ByteBuffer] = {
    val buffer: Array[Byte] = new Array[Byte](pmofConf.reduce_serializer_buffer_size.toInt)
    var output = new Output(buffer)

    val bufferArray = new ArrayBuffer[ByteBuffer]()
    val totalBlock = byteBuffer.getInt()
    var cur = 0
    var pre = -1
    var psbi: String = null
    var csbi: String = null
    MetadataResolver.this.synchronized {
      try {
        while (cur < totalBlock) {
          if (cur == pre) {
            csbi = psbi
          } else {
            csbi = ShuffleBlockId(byteBuffer.getInt(), byteBuffer.getInt(), byteBuffer.getInt()).name
            psbi = csbi
            pre = cur
          }
          if (blockInfoMap.contains(csbi)) {
            val blockInfoArray = blockInfoMap(csbi)
            val startPos = output.position()
            val loop = Breaks
            loop.breakable {
              for (i <- blockInfoArray.indices) {
                info_serialize_stream.writeObject(output, blockInfoArray(i))
                if (output.position() >= pmofConf.reduce_serializer_buffer_size * 0.9) {
                  output.setPosition(startPos)
                  val blockBuffer = ByteBuffer.wrap(output.getBuffer)
                  blockBuffer.position(output.position())
                  blockBuffer.flip()
                  bufferArray += blockBuffer
                  output.close()
                  val new_buffer = new Array[Byte](pmofConf.reduce_serializer_buffer_size.toInt)
                  output = new Output(new_buffer)
                  cur -= 1
                  loop.break()
                }
              }
            }
          }
          cur += 1
        }
      } catch {
        case c: ControlThrowable => throw c
        case t: Throwable => t.printStackTrace()
      }
      if (output.position() != 0) {
        val blockBuffer = ByteBuffer.wrap(output.getBuffer)
        blockBuffer.position(output.position())
        blockBuffer.flip()
        bufferArray += blockBuffer
        output.close()
      }
    }
    bufferArray
  }

  /**
    * called by executor, deserialize bytebuffer to shuffle block metadata object
    * @param byteBuffer
    * @return
    */
  def deserializeShuffleBlockInfo(byteBuffer: ByteBuffer): ArrayBuffer[ShuffleBlockInfo] = {
    val blockInfoArray: ArrayBuffer[ShuffleBlockInfo] = ArrayBuffer[ShuffleBlockInfo]()
    val bais = new ByteBufferInputStream(byteBuffer)
    val input = new Input(bais)
    MetadataResolver.this.synchronized {
      do {
        if (input.available() > 0) {
          val shuffleBlockInfo = info_serialize_stream.readObject(input, classOf[ShuffleBlockInfo])
          blockInfoArray += shuffleBlockInfo
        } else {
          input.close()
          return blockInfoArray
        }
      } while (true)
    }
    null
  }

  def closeBlocks(): Unit = {
    for ((_, v) <- blockMap.asScala) {
      v.close()
    }
  }
}

object MetadataResolver {
  private[this] final val initialized = new AtomicBoolean(false)
  private[this] var metadataResolver: MetadataResolver = _

  def getMetadataResolver(pmofConf: PmofConf): MetadataResolver = {
    if (!initialized.get()) {
      MetadataResolver.this.synchronized {
        if (initialized.get()) return metadataResolver
        metadataResolver = new MetadataResolver(pmofConf)
        initialized.set(true)
        metadataResolver
      }
    } else {
      metadataResolver
    }
  }
}
