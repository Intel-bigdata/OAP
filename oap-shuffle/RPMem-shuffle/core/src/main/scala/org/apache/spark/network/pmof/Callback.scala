package org.apache.spark.network.pmof

import scala.collection.mutable.ArrayBuffer

trait ReadCallback {
  def onSuccess(buffer: ShuffleBuffer, f: (Int) => Unit): Unit
  def onFailure(e: Throwable): Unit
}

trait ReceivedCallback {
  def onSuccess(obj: ArrayBuffer[ShuffleBlockInfo]): Unit
  def onFailure(e: Throwable): Unit
}
