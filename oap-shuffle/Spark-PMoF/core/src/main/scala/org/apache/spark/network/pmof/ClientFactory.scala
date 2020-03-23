package org.apache.spark.network.pmof

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.intel.hpnl.core._
import org.apache.spark.shuffle.pmof.PmofShuffleManager
import org.apache.spark.util.configuration.pmof.PmofConf

class ClientFactory(pmofConf: PmofConf) {
  final val eqService = new EqService(pmofConf.clientWorkerNums, pmofConf.clientBufferNums, false).init()
  private[this] final val cqService = new CqService(eqService).init()
  private[this] final val clientMap = new ConcurrentHashMap[InetSocketAddress, Client]()
  private[this] final val conMap = new ConcurrentHashMap[Connection, Client]()

  def init(): Unit = {
    eqService.initBufferPool(pmofConf.clientBufferNums, pmofConf.networkBufferSize, pmofConf.clientBufferNums * 2)
    val clientRecvHandler = new ClientRecvHandler
    val clientReadHandler = new ClientReadHandler
    eqService.setRecvCallback(clientRecvHandler)
    eqService.setReadCallback(clientReadHandler)
    cqService.start()
  }

  def createClient(shuffleManager: PmofShuffleManager, address: String, port: Int): Client = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    var client = clientMap.get(socketAddress)
    if (client == null) {
      ClientFactory.this.synchronized {
        client = clientMap.get(socketAddress)
        if (client == null) {
          val con = eqService.connect(address, port.toString, 0)
          client = new Client(this, shuffleManager, con)
          clientMap.put(socketAddress, client)
          conMap.put(con, client)
        }
      }
    }
    client
  }

  def stop(): Unit = {
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def getEqService: EqService = eqService

  class ClientRecvHandler() extends Handler {
    override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
      val hpnlBuffer: HpnlBuffer = con.getRecvBuffer(rdmaBufferId)
      val byteBuffer: ByteBuffer = hpnlBuffer.get(blockBufferSize)
      val seq = hpnlBuffer.getSeq
      val msgType = hpnlBuffer.getType
      val callback = conMap.get(con).outstandingReceiveFetches.get(seq)
      if (msgType == 0.toByte) { // get ACK from driver, which means the block info has been saved to driver memory
        callback.onSuccess(null)
      } else { // get block info from driver, and deserialize the info to scala object
        val metadataResolver = conMap.get(con).shuffleManager.metadataResolver
        val blockInfoArray = metadataResolver.deserializeShuffleBlockInfo(byteBuffer)
        callback.onSuccess(blockInfoArray)
      }
    }
  }

  class ClientReadHandler() extends Handler {
    override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
      def fun(v1: Int): Unit = {
        conMap.get(con).shuffleBufferMap.remove(v1)
        conMap.get(con).outstandingReadFetches.remove(v1)
      }

      val callback = conMap.get(con).outstandingReadFetches.get(rdmaBufferId)
      val shuffleBuffer = conMap.get(con).shuffleBufferMap.get(rdmaBufferId)
      callback.onSuccess(shuffleBuffer, fun)
    }
  }
}
