/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import org.apache.spark.rpc.{ThreadSafeRpcEndpoint, RpcEnv, RpcCallContext}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock}

/**
 * Fiber Update from executors to the driver.
 */
private[spark] case class FiberUpdateHeartBeat(
    executorId: String,
    updateInfo: String,
    blockManagerId: BlockManagerId)

private[spark] case class FiberUpdateResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive Fiber Update info from executors..
 */
private[spark] class FiberUpdateReceiver(
    sc: SparkContext,
    listenerBus: LiveListenerBus,
    clock: Clock) extends ThreadSafeRpcEndpoint with Logging {

  def this(sc: SparkContext) {
    this(sc, sc.listenerBus, new SystemClock)
  }

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // Messages received from executors
    case fiberUpdate @ FiberUpdateHeartBeat(executorId, updateInfo, blockManagerId) =>
      listenerBus.post(SparkListenerExecutorFiberUpdate(executorId, updateInfo, blockManagerId))
      context.reply(FiberUpdateResponse(reregisterBlockManager = false))
    case _ => // do nothing if it is not Fiber Update
  }

}

object FiberUpdateReceiver {
  val ENDPOINT_NAME = "FiberUpdateReceiver"
}

/**
 * This class only used for test
 */
class DummySparkListener() extends SparkListener with Logging {
  override def onFiberUpdate(fiberUpdate: SparkListenerExecutorFiberUpdate): Unit = {
    logInfo(s"${fiberUpdate.fiberUpdateInfo}, local time: ${System.currentTimeMillis()}")
  }
}
