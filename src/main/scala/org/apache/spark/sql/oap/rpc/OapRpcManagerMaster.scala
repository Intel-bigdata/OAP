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

package org.apache.spark.sql.oap.rpc

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.oap.listener.{SparkListenerCustomInfoUpdate, SparkListenerOapIndexInfoUpdate}
import org.apache.spark.sql.oap.rpc.OapMessages._

/**
 * An OapRpcManager running on Driver to send messages to Executors, get this object from SparkEnv
 * @param oapRpcManagerMasterEndpoint The OapRpcManagerMasterEndpoint it holds, dealing with
 *                                    messages' receiving
 */
private[spark] class OapRpcManagerMaster(oapRpcManagerMasterEndpoint: OapRpcManagerMasterEndpoint)
  extends OapRpcManager with Logging {

  private def sendOneWayMessageToExecutors(message: OapMessage): Unit = {
    oapRpcManagerMasterEndpoint.rpcEndpointRefByExecutor.foreach {
      case (_, slaveEndpoint) => slaveEndpoint.send(message)
    }
  }

  override private[spark] def send(message: OapMessage): Unit = {
    sendOneWayMessageToExecutors(message)
  }
}

private[spark] object OapRpcManagerMaster {
  val DRIVER_ENDPOINT_NAME = "OapRpcManagerMaster"
}

private[spark] class OapRpcManagerMasterEndpoint(
    override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from executor ID to RpcEndpointRef.
  private[rpc] val rpcEndpointRefByExecutor = new mutable.HashMap[String, RpcEndpointRef]

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterOapRpcManager(executorId, slaveEndpoint) =>
      context.reply(handleRegistration(executorId, slaveEndpoint))
    case _ =>
  }

  override def receive: PartialFunction[Any, Unit] = {
    case heartbeat: Heartbeat => handleHeartbeat(heartbeat)
    case message: OapMessage => handleNormalOapMessage(message)
    case _ =>
  }

  private def handleRegistration(executorId: String, ref: RpcEndpointRef): Boolean = {
    rpcEndpointRefByExecutor += ((executorId, ref))
    true
  }

  private def handleNormalOapMessage(message: OapMessage) = message match {
    case _: Heartbeat => throw new IllegalArgumentException(
      "This is only to deal with non-heartbeat messages")
    case DummyMessage(id, someContent) =>
      val c = this.getClass.getMethods
      logWarning(s"Dummy message received on Driver with id: $id, content: $someContent")
    case _ =>
  }

  private def handleHeartbeat(heartbeat: Heartbeat) = heartbeat match {
    case FiberCacheHeartbeat(executorId, blockManagerId, content) =>
      SparkContext.getOrCreate().listenerBus.post(SparkListenerCustomInfoUpdate(
        blockManagerId.host, executorId, "OapFiberCacheHeartBeatMessager", content))
    case FiberCacheMetricsHeartbeat(executorId, blockManagerId, content) =>
      SparkContext.getOrCreate().listenerBus.post(SparkListenerCustomInfoUpdate(
        blockManagerId.host, executorId, "FiberCacheManagerMessager", content))
    case IndexHeartbeat(executorId, blockManagerId, content) =>
      SparkContext.getOrCreate().listenerBus.post(SparkListenerOapIndexInfoUpdate(
        blockManagerId.host, executorId, content))
    case _ =>
  }
}
