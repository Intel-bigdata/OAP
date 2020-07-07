package org.apache.spark.network.pmof

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.BlockStoreClient

abstract class TransferService extends BlockStoreClient{
  def init(blockDataManager: BlockDataManager): Unit

  def close(): Unit

  def hostname: String

  def port: Int
}
