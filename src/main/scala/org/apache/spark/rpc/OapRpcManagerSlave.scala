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

package org.apache.spark.rpc

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.OapMessages._
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Similar OapRpcManager class with [[OapRpcManagerMaster]], however running on Executor
 */
private[spark] class OapRpcManagerSlave(
  rpcEnv: RpcEnv, val driverEndpoint: RpcEndpointRef, executorId: String, conf: SparkConf)
    extends OapRpcManager {

  // Send OapHeartbeatMessage to Driver timed
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    s"OapRpcManagerSlave_$executorId", new OapRpcManagerSlaveEndpoint(rpcEnv))

  initialize()

  private def initialize() = {
    driverEndpoint.askWithRetry[Boolean](RegisterOapRpcManager(executorId, slaveEndpoint))
  }

  override private[spark] def send(message: OapMessage): Unit = driverEndpoint.send(message)

  private def reportHeartBeat(getMaterials: Seq[() => HeartBeat]): Unit = {
    val materials = getMaterials.map(_.apply())
    materials.foreach(send)
  }

  private[spark] def sendHearBeat(getMaterials: Seq[() => HeartBeat]): Unit = {
    val MS = 1000
    val intervalMs = MS * conf.getInt(
          OapConf.OAP_HEARTBEAT_INTERVAL.key, OapConf.OAP_HEARTBEAT_INTERVAL.defaultValue.get)

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat(getMaterials))
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] class OapRpcManagerSlaveEndpoint(override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case message: OapMessage => handleOapMessage(message)
    case _ =>
  }

  private def handleOapMessage(message: OapMessage): Unit = message match {
    case MyDummyMessage(id, someContent) =>
      logWarning(s"Dummy message received on Executor with id: $id, content: $someContent")
    case CacheDrop(indexName) => FiberCacheManager.removeIndexCache(indexName)
    case _ =>
  }
}
