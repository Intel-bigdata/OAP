package org.apache.spark.storage.pmof

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.network.pmof.PmofTransferService
import org.apache.spark.SparkEnv

import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.lang.management.ManagementFactory

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.util.configuration.pmof.PmofConf

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

private[spark] class PersistentMemoryHandler(
    val root_dir: String,
    val path_list: List[String],
    val shuffleId: String,
    var poolSize: Long = -1) extends Logging {
  // need to use a locked file to get which pmem device should be used.
  val pmMetaHandler: PersistentMemoryMetaHandler = new PersistentMemoryMetaHandler(root_dir)
  var device: String = pmMetaHandler.getShuffleDevice(shuffleId)
  if(device == "") {
    //this shuffleId haven't been written before, choose a new device
    val path_array_list = new java.util.ArrayList[String](path_list.asJava)
    device = pmMetaHandler.getUnusedDevice(path_array_list)

    val dev = Paths.get(device)
    if (Files.isDirectory(dev)) {
      // this is fsdax, add a subfile
      device += "/shuffle_block_" + UUID.randomUUID().toString()
      logInfo("This is a fsdax, filename:" + device)
    } else {
      logInfo("This is a devdax, name:" + device)
      poolSize = 0
    }
  }
  
  val pmpool = new PersistentMemoryPool(device, poolSize)
  var rkey: Long = 0


  def getDevice(): String = {
    device
  }

  def updateShuffleMeta(shuffleId: String): Unit = synchronized {
    pmMetaHandler.insertRecord(shuffleId, device);
  }

  def getPartitionBlockInfo(blockId: String): Array[(Long, Int)] = {
    var res_array: Array[Long] = pmpool.getPartitionBlockInfo(blockId)
    var i = -2
    var blockInfo = Array.ofDim[(Long, Int)]((res_array.length)/2)
    blockInfo.map{
      x => i += 2;
      (res_array(i), res_array(i+1).toInt)
    }
  }

  def getPartitionSize(blockId: String): Long = {
    pmpool.getPartitionSize(blockId)
  }
  
  def setPartition(numPartitions: Int, blockId: String, byteBuffer: ByteBuffer, size: Int, clean: Boolean): Unit = {
    pmpool.setPartition(blockId, byteBuffer, size, clean)
  }

  def deletePartition(blockId: String): Unit = {
    pmpool.deletePartition(blockId)
  }

  def getPartitionManagedBuffer(blockId: String): ManagedBuffer = {
    new PmemManagedBuffer(this, blockId)
  }

  def close(): Unit = synchronized {
    pmpool.close()
    pmMetaHandler.remove()
  }

  def getRootAddr(): Long = {
    pmpool.getRootAddr();
  }

  def log(printout: String) {
    logInfo(printout)
  }
}

object PersistentMemoryHandler {
  private var persistentMemoryHandler: PersistentMemoryHandler = _
  private var stopped: Boolean = _
  def getPersistentMemoryHandler(pmofConf: PmofConf, root_dir: String, path_arg: List[String], shuffleBlockId: String, pmPoolSize: Long): PersistentMemoryHandler = synchronized {
    if (persistentMemoryHandler == null) {
      persistentMemoryHandler = new PersistentMemoryHandler(root_dir, path_arg, shuffleBlockId, pmPoolSize)
      persistentMemoryHandler.log("Use persistentMemoryHandler Object: " + this)
      if (pmofConf.enableRdma) {
        val blockManager = SparkEnv.get.blockManager
        val eqService = PmofTransferService.getTransferServiceInstance(pmofConf, blockManager).server.getEqService
        val offset: Long = persistentMemoryHandler.getRootAddr
        val rdmaBuffer = eqService.regRmaBufferByAddress(null, offset, pmofConf.pmemCapacity)
        persistentMemoryHandler.rkey = rdmaBuffer.getRKey()
      }
      val core_set = pmofConf.pmemCoreMap.get(persistentMemoryHandler.getDevice())
      core_set match {
        case Some(s) => Future {nativeTaskset(s)}
        case None => {}
      }
      stopped = false
    }
    persistentMemoryHandler
  }

  def getPersistentMemoryHandler: PersistentMemoryHandler = synchronized {
    if (persistentMemoryHandler == null) {
      throw new NullPointerException("persistentMemoryHandler")
    }
    persistentMemoryHandler
  }

  def stop(): Unit = synchronized {
    if (!stopped && persistentMemoryHandler != null) {
      persistentMemoryHandler.close()
      persistentMemoryHandler = null
      stopped = true
    }
  }

  def nativeTaskset(core_set: String): Unit = {
    Runtime.getRuntime.exec("taskset -cpa " + core_set + " " + getProcessId())
  }

  def getProcessId(): Int = {
    val runtimeMXBean = ManagementFactory.getRuntimeMXBean()
    runtimeMXBean.getName().split("@")(0).toInt
  }
}
