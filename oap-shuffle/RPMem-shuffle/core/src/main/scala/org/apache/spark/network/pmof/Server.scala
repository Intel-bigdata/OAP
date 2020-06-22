package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util

import com.intel.hpnl.core._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.pmof.PmofShuffleManager
import org.apache.spark.util.configuration.pmof.PmofConf

class Server(pmofConf: PmofConf, val shuffleManager: PmofShuffleManager, address: String, var port: Int) {
  if (port == 0) {
    port = Utils.getPort
  }

  private[this] final val eqService = new EqService(pmofConf.serverWorkerNums, pmofConf.serverBufferNums, true).init()
  private[this] final val cqService = new CqService(eqService).init()

  private[this] final val conList = new util.ArrayList[Connection]()

  def init(): Unit = {
    eqService.initBufferPool(pmofConf.serverBufferNums, pmofConf.networkBufferSize, pmofConf.serverBufferNums * 2)
    val recvHandler = new ServerRecvHandler(this)
    val connectedHandler = new ServerConnectedHandler(this)
    eqService.setConnectedCallback(connectedHandler)
    eqService.setRecvCallback(recvHandler)
  }

  def start(): Unit = {
    cqService.start()
    eqService.listen(address, port.toString)
  }

  def stop(): Unit = {
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def getEqService: EqService = {
    eqService
  }

  def addCon(con: Connection): Unit = synchronized {
    conList.add(con)
  }
}

class ServerRecvHandler(server: Server) extends Handler with Logging {

  private final val byteBufferTmp = ByteBuffer.allocate(4)

  def sendMetadata(con: Connection, byteBuffer: ByteBuffer, msgType: Byte, seq: Long, isDeferred: Boolean): Unit = {
    con.send(byteBuffer, msgType, seq)
  }

  override def handle(con: Connection, bufferId: Int, blockBufferSize: Int): Unit = synchronized {
    val hpnlBuffer: HpnlBuffer = con.getRecvBuffer(bufferId)
    val byteBuffer: ByteBuffer = hpnlBuffer.get(blockBufferSize)
    val seq = hpnlBuffer.getSeq
    val msgType = hpnlBuffer.getType
    val metadataResolver = server.shuffleManager.metadataResolver
    if (msgType == 0.toByte) { // get block info message from executor, then save the info to memory
      metadataResolver.saveShuffleBlockInfo(byteBuffer)
      sendMetadata(con, byteBufferTmp, 0.toByte, seq, isDeferred = false)
    } else { // lookup block info from memory, then send the info to executor
      val blockInfoArray = metadataResolver.serializeShuffleBlockInfo(byteBuffer)
      for (buffer <- blockInfoArray) {
        sendMetadata(con, buffer, 1.toByte, seq, isDeferred = false)
      }
    }
  }
}

class ServerConnectedHandler(server: Server) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, bufferBufferSize: Int): Unit = {
    server.addCon(con)
  }
}
