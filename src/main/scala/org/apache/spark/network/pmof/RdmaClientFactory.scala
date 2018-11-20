package org.apache.spark.network.pmof

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkConf

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.util.Random

class RdmaClientFactory(conf: SparkConf) {
  final val CON_NUM: Int = conf.getInt("spark.shuffle.pmof.client_pool_size", 1)
  val conPools: concurrent.Map[SocketAddress, RdmaClientPool] =
    new ConcurrentHashMap[SocketAddress, RdmaClientPool]().asScala

  def createClient(address: String, port: Int): RdmaClient = synchronized {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    val conPool: RdmaClientPool = conPools.getOrElse(socketAddress, {
      val conPool = new RdmaClientPool(conf, CON_NUM, address, port)
      conPools.put(socketAddress, conPool)
      conPool
    })
    conPool.get(new Random().nextInt(CON_NUM))
  }

  def stop(): Unit = {
    conPools.foreach(_._2.stop())
  }

  def waitToStop(): Unit = {
    conPools.foreach(_._2.waitToStop())
  }
}
