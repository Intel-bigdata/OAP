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

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.execution.datasources.oap.filecache.{CacheStats, FiberCacheManager}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.adapter.RpcEndpointRefAdapter
import org.apache.spark.sql.oap.rpc.OapMessages._
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Similar OapRpcManager class with [[OapRpcManagerMaster]], however running on Executor
 */
private[spark] class OapRpcManagerSlave(
    rpcEnv: RpcEnv,
    val driverEndpoint: RpcEndpointRef,
    executorId: String,
    blockManager: BlockManager,
    fiberCacheManager: FiberCacheManager,
    conf: SparkConf) extends OapRpcManager {

  // Send OapHeartbeatMessage to Driver timed
  private val oapHeartbeater =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    s"OapRpcManagerSlave_$executorId", new OapRpcManagerSlaveEndpoint(rpcEnv, fiberCacheManager))

  initialize()
  startOapHeartbeater()

  protected def heartbeatMessages: Array[() => Heartbeat] = {
    Array(
      () => FiberCacheHeartbeat(
        executorId, blockManager.blockManagerId, fiberCacheManager.status()),
      () => FiberCacheMetricsHeartbeat(executorId, blockManager.blockManagerId,
        CacheStats.status(fiberCacheManager.cacheStats, conf)))
  }

  protected def metricsHeartbeatMessages: Array[() => Heartbeat] = {
    return Array(
      () => FiberCacheMetricsHeartbeat(executorId, blockManager.blockManagerId,
        CacheStats.status(fiberCacheManager.cacheStats, conf)))
  }

  private def initialize() = {
    RpcEndpointRefAdapter.askSync[Boolean](
      driverEndpoint, RegisterOapRpcManager(executorId, slaveEndpoint))
  }

  override private[spark] def send(message: OapMessage): Unit = {
    driverEndpoint.send(message)
  }

  private[sql] def startOapHeartbeater(): Unit = {

    def reportHeartbeat(): Unit = {
      // OapRpcManagerSlave is created in SparkEnv. Before we start the heartbeat, we need make
      // sure the SparkEnv has been created and the block manager has been initialized. We check
      // blockManagerId as it will be set after initialization.
      if (blockManager.blockManagerId != null) {
        heartbeatMessages.map(_.apply()).foreach(send)
      }
    }

    def reportMetricsHeartbeat(): Unit = {
      if (blockManager.blockManagerId != null) {
        metricsHeartbeatMessages.map(_.apply()).foreach(send)
      }
    }

    val intervalMs = conf.getTimeAsMs(
      OapConf.OAP_HEARTBEAT_INTERVAL.key, OapConf.OAP_HEARTBEAT_INTERVAL.defaultValue.get)

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartbeat())
    }

    val metricsHeartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportMetricsHeartbeat())
    }

    if (!SparkEnv.get.conf.get(OapConf.OAP_EXTERNAL_CACHE_METADB_ENABLED)) {
      oapHeartbeater.scheduleAtFixedRate(
        heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
    } else {
      oapHeartbeater.scheduleAtFixedRate(
        metricsHeartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
    }
  }

  override private[spark] def stop(): Unit = {
    oapHeartbeater.shutdown()
  }
}

private[spark] class OapRpcManagerSlaveEndpoint(
    override val rpcEnv: RpcEnv, fiberCacheManager: FiberCacheManager)
  extends ThreadSafeRpcEndpoint with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case message: OapMessage => handleOapMessage(message)
    case _ =>
  }

  private def handleOapMessage(message: OapMessage): Unit = message match {
    case CacheDrop(indexName) => fiberCacheManager.releaseIndexCache(indexName)
    case _ =>
  }
}
