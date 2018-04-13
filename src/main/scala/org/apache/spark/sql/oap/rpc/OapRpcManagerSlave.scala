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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.rpc.OapMessages._
import org.apache.spark.util.{ThreadUtils, Utils}


/**
 * Similar OapRpcManager class with [[OapRpcManagerMaster]], however running on Executor
 */
private[spark] class OapRpcManagerSlave(
    rpcEnv: RpcEnv, val driverEndpoint: RpcEndpointRef, executorId: String, conf: SparkConf)
        extends OapRpcManager {

  // Send OapHeartbeatMessage to Driver timed
  private val oapHeartbeater =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  private val oapHeartbeatMaterials = mutable.HashSet.empty[() => Heartbeat]

  private def getHeartbeatMaterials = oapHeartbeatMaterials

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    s"OapRpcManagerSlave_$executorId", new OapRpcManagerSlaveEndpoint(rpcEnv))

  initialize()

  startOapHeartbeater()

  private def initialize() = {
    driverEndpoint.askWithRetry[Boolean](RegisterOapRpcManager(executorId, slaveEndpoint))
  }

  override private[spark] def send(message: OapMessage): Unit = driverEndpoint.send(message)

  private def startOapHeartbeater(): Unit = {

    def reportHeartbeat(): Unit = {
      // When the object is just initialized, this is empty, elements add to it after
      // registerHeartbeat is called
      val getMaterials = getHeartbeatMaterials.toSeq
      val materials = getMaterials.map(_.apply())
      materials.foreach(send)
    }

    val intervalMs = conf.getTimeAsMs(
      OapConf.OAP_HEARTBEAT_INTERVAL.key, OapConf.OAP_HEARTBEAT_INTERVAL.defaultValue.get)

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartbeat())
    }
    oapHeartbeater.scheduleAtFixedRate(
      heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }

  private[spark] def registerHearbeat(getMaterials: Seq[() => Heartbeat]): Unit = {
    getMaterials.foreach(oapHeartbeatMaterials += _)
  }

  override private[spark] def stop(): Unit = {
    oapHeartbeater.shutdown()
  }
}

private[spark] class OapRpcManagerSlaveEndpoint(override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case message: OapMessage => handleOapMessage(message)
    case _ =>
  }

  private def handleOapMessage(message: OapMessage): Unit = message match {
    case DummyMessage(id, someContent) =>
      logWarning(s"Dummy message received on Executor with id: $id, content: $someContent")
    case CacheDrop(indexName) => FiberCacheManager.removeIndexCache(indexName)
    case _ =>
  }
}
