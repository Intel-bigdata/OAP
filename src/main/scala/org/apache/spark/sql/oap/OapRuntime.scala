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

package org.apache.spark.sql.oap

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.OapMetricsManager
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.hive.thriftserver.OapEnv
import org.apache.spark.sql.oap.rpc._
import org.apache.spark.util.{RpcUtils, Utils}


/**
 * Initializing [[FiberCacheManager]], [[MemoryManager]], [[FiberLockManager]]
 * [[FiberSensor]], [[OapMetricsManager]], [[OapRpcManager]], [[DataFileMetaCacheManager]]
 */
private[oap] trait OapRuntime extends Logging {
  def memoryManager: MemoryManager
  def fiberCacheManager: FiberCacheManager
  def fiberSensor: FiberSensor = null
  def oapRpcManager: OapRpcManager
  def oapMetricsManager: OapMetricsManager
  def dataFileMetaCacheManager: DataFileMetaCacheManager
  def fiberLockManager: FiberLockManager
  def stop(): Unit
}

/**
 * Initializing [[FiberSensor]] and executor managers if local
 */
private[oap] class OapDriverRuntime(sparkEnv: SparkEnv) extends OapRuntime {

  // For non-Spark SQL CLI/ThriftServer conditions, OAP-specific features will be fully enabled by
  // this, nevertheless not instantly when a Spark application is started, but when an OapRuntime
  // is created. For example, OAP UI tab will show at the moment you read a Parquet file to OAP
  // cache
  OapEnv.initWithoutCreatingOapSession()

  override val memoryManager =
    if (OapRuntime.isLocal(sparkEnv.conf)) new MemoryManager(sparkEnv) else null
  override val fiberCacheManager =
    if (OapRuntime.isLocal(sparkEnv.conf)) new FiberCacheManager(sparkEnv, memoryManager) else null
  override val fiberSensor = new FiberSensor
  private val oapRpcManagerMasterEndpoint =
    new OapRpcManagerMasterEndpoint(sparkEnv.rpcEnv, SparkContext.getOrCreate().listenerBus)
  private val oapRpcDriverEndpoint = {
    logInfo("Registering " + OapRpcManagerMaster.DRIVER_ENDPOINT_NAME)
    sparkEnv.rpcEnv.setupEndpoint(
      OapRpcManagerMaster.DRIVER_ENDPOINT_NAME, oapRpcManagerMasterEndpoint)
  }
  override val oapRpcManager = if (!OapRuntime.isLocal(sparkEnv.conf)) {
    new OapRpcManagerMaster(oapRpcManagerMasterEndpoint)
  } else {
    new OapRpcManagerSlave(
      sparkEnv.rpcEnv,
      oapRpcDriverEndpoint,
      sparkEnv.executorId,
      sparkEnv.blockManager,
      fiberCacheManager,
      sparkEnv.conf)
  }
  override val oapMetricsManager = new OapMetricsManager
  // TODO are the following needed for driver?
  override val dataFileMetaCacheManager = new DataFileMetaCacheManager
  override val fiberLockManager = new FiberLockManager

  override def stop(): Unit = {
    if (OapRuntime.isLocal(sparkEnv.conf)) {
      memoryManager.stop()
    }
    if (OapRuntime.isLocal(sparkEnv.conf)) {
      fiberCacheManager.stop()
    }
    oapRpcManager.stop()
    dataFileMetaCacheManager.stop()
  }
}

/**
 * Initializing [[FiberCacheManager]], [[MemoryManager]]
 */
private[oap] class OapExecutorRuntime(sparkEnv: SparkEnv) extends OapRuntime {
  override val memoryManager = new MemoryManager(sparkEnv)
  override val fiberCacheManager = new FiberCacheManager(sparkEnv, memoryManager)
  private val oapRpcDriverEndpoint = RpcUtils.makeDriverRef(
    OapRpcManagerMaster.DRIVER_ENDPOINT_NAME, sparkEnv.conf, sparkEnv.rpcEnv)
  override val oapRpcManager = new OapRpcManagerSlave(
    sparkEnv.rpcEnv,
    oapRpcDriverEndpoint,
    sparkEnv.executorId,
    sparkEnv.blockManager,
    fiberCacheManager,
    sparkEnv.conf)
  override val oapMetricsManager = new OapMetricsManager
  override val dataFileMetaCacheManager = new DataFileMetaCacheManager
  override val fiberLockManager = new FiberLockManager

  override def stop(): Unit = {
    memoryManager.stop()
    fiberCacheManager.stop()
    oapRpcManager.stop()
    dataFileMetaCacheManager.stop()
  }
}

object OapRuntime {

  private var rt: OapRuntime = _
  /**
   * user transparent initialization
   */
  def getOrCreate: OapRuntime = if (rt == null) init() else rt

  def get: Option[OapRuntime] = Option(rt)

  def init(): OapRuntime = {
    val sparkEnv = SparkEnv.get
    if (sparkEnv == null) throw new OapException("Can't run OAP without SparkContext")
    init(sparkEnv)
  }

  // init() is not called in SparkEnv because maybe user don't want to keep
  // OapRuntime always ready, since Oap can take a lot of memory. By manually call stop(),
  // user can delete every instance of OAP, use stock spark without restart cluster.
  // Now we rely on SparkEnv to call stop() for us.
  def init(sparkEnv: SparkEnv): OapRuntime = synchronized {
    if (rt == null) {
      rt = if (isDriver(sparkEnv.conf)) {
        new OapDriverRuntime(sparkEnv)
      } else {
        new OapExecutorRuntime(sparkEnv)
      }
    }
    rt
  }

  private[oap] def isDriver(conf: SparkConf): Boolean = {
    conf.get("spark.executor.id") == SparkContext.DRIVER_IDENTIFIER
  }

  private[oap] def isLocal(conf: SparkConf): Boolean = {
    Utils.isLocalMaster(conf)
  }

  /**
   * Create a new OapEnv with latest SparkConf. For test purpose
   */
  def createOrUpdate(): Unit = {
    stop()
    init()
  }

  def stop(): Unit = {
    if (rt != null) {
      val runtime = rt
      // set to null before actual stop, so when we get OapRuntime is not null,
      // we can always use it.
      rt = null
      runtime.stop()
    }
  }
}
