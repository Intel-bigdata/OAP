package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.intel.hpnl.core.{Connection, EqService}
import org.apache.spark.shuffle.pmof.PmofShuffleManager

class Client(clientFactory: ClientFactory, val shuffleManager: PmofShuffleManager, con: Connection) {
  final val outstandingReceiveFetches: ConcurrentHashMap[Long, ReceivedCallback] =
    new ConcurrentHashMap[Long, ReceivedCallback]()
  final val outstandingReadFetches: ConcurrentHashMap[Int, ReadCallback] =
    new ConcurrentHashMap[Int, ReadCallback]()
  final val shuffleBufferMap: ConcurrentHashMap[Int, ShuffleBuffer] = new ConcurrentHashMap[Int, ShuffleBuffer]()

  def getEqService: EqService = clientFactory.eqService

  def read(shuffleBuffer: ShuffleBuffer, reqSize: Int,
           rmaAddress: Long, rmaRkey: Long, localAddress: Int,
           callback: ReadCallback, isDeferred: Boolean = false): Unit = {
    if (!isDeferred) {
      outstandingReadFetches.putIfAbsent(shuffleBuffer.getRdmaBufferId, callback)
      shuffleBufferMap.putIfAbsent(shuffleBuffer.getRdmaBufferId, shuffleBuffer)
    }
    con.read(shuffleBuffer.getRdmaBufferId, localAddress, reqSize, rmaAddress, rmaRkey)
  }

  def send(byteBuffer: ByteBuffer, seq: Long, msgType: Byte,
           callback: ReceivedCallback, isDeferred: Boolean): Unit = {
    assert(con != null)
    if (callback != null) {
      outstandingReceiveFetches.putIfAbsent(seq, callback)
    }
    con.send(byteBuffer, msgType, seq)
  }
}
