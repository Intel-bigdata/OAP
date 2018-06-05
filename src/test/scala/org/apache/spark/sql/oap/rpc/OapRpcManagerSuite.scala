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

import org.mockito.Mockito._
import org.mockito.internal.verification.AtLeast
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}

import org.apache.spark._
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.rpc.OapMessages.{DummyHeartbeat, DummyMessage, Heartbeat, RegisterOapRpcManager}


class OapRpcManagerSuite extends SparkFunSuite with BeforeAndAfterEach with PrivateMethodTester
    with LocalSparkContext {

  private var rpcEnv: RpcEnv = _

  private val executorId1 = "executor-1"
  private val executorId2 = "executor-2"

  // Shared state that must be reset before and after each test
  private var rpcManagerMasterEndpoint: OapRpcManagerMasterEndpoint = _
  private var rpcManagerMaster: OapRpcManagerMaster = _
  private var rpcDriverEndpoint: RpcEndpointRef = _

  // Helper private method accessors for OapRpcManagerMaster
  private val _handleNormalOapMessage = PrivateMethod[Unit]('handleNormalOapMessage)
  private val _handleHeartbeat = PrivateMethod[Unit]('handleHeartbeat)
  // Helper private method accessors for OapRpcManagerSlave
  private val _handleOapMessage = PrivateMethod[Unit]('handleOapMessage)

  private val idAndContent = ("0", "He is a wanderer")
  private val message = DummyMessage.tupled(idAndContent)
  private val heartbeat: Heartbeat = DummyHeartbeat(idAndContent._2)

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")

    sc = new SparkContext(conf)
    rpcEnv = sc.env.rpcEnv
    rpcManagerMasterEndpoint = spy(new OapRpcManagerMasterEndpoint(rpcEnv, sc.listenerBus))
    rpcManagerMaster = new OapRpcManagerMaster(rpcManagerMasterEndpoint)
    rpcDriverEndpoint = rpcEnv.setupEndpoint("driver", rpcManagerMasterEndpoint)
  }

  /**
   * After each test, clean up all state and stop the [[SparkContext]].
   */
  override def afterEach(): Unit = {
    super.afterEach()
    sc = null
    rpcEnv = null
    rpcManagerMasterEndpoint = null
    rpcManagerMaster = null
    rpcDriverEndpoint = null
  }

  test("Send message from Driver to Executors") {
    val rpcManagerSlaveEndpoint1 = addSpiedRpcManagerSlaveEndpoint(executorId1)
    val rpcManagerSlaveEndpoint2 = addSpiedRpcManagerSlaveEndpoint(executorId2)

    rpcManagerMaster.send(message)
    Thread.sleep(2000)
    verify(rpcManagerSlaveEndpoint1).invokePrivate(_handleOapMessage(message))
    verify(rpcManagerSlaveEndpoint2).invokePrivate(_handleOapMessage(message))
  }

  test("Send non-heartbeat message from Executor to Driver") {
    val rpcManagerSlave1 = addRpcManagerSlave(executorId1)

    rpcManagerSlave1.send(message)
    Thread.sleep(2000)
    verify(rpcManagerMasterEndpoint).invokePrivate(_handleNormalOapMessage(message))
    rpcManagerSlave1.stop()
  }

  test("Send heartbeat message from Executor to Driver") {

    val rpcManagerSlave1 = addRpcManagerSlave(executorId1)

    // Initial delay is at most 2 * interval
    Thread.sleep(2000 + 2 * sc.conf.getTimeAsMs(
      OapConf.OAP_HEARTBEAT_INTERVAL.key, OapConf.OAP_HEARTBEAT_INTERVAL.defaultValue.get))
    verify(rpcManagerMasterEndpoint, new AtLeast(1)).invokePrivate(_handleHeartbeat(heartbeat))

    rpcManagerSlave1.stop()
  }

  private def addSpiedRpcManagerSlaveEndpoint(executorId: String): OapRpcManagerSlaveEndpoint = {
    val rpcManagerSlaveEndpoint = spy(new OapRpcManagerSlaveEndpoint(rpcEnv, null))
    val slaveEndpoint = rpcEnv.setupEndpoint(
      s"OapRpcManagerSlave_$executorId", rpcManagerSlaveEndpoint)
    rpcDriverEndpoint.askWithRetry[Boolean](RegisterOapRpcManager(executorId, slaveEndpoint))
    rpcManagerSlaveEndpoint
  }

  // This doesn't need to be spied due to it's used to send messages
  private def addRpcManagerSlave(executorId: String): OapRpcManagerSlave = {
    new OapRpcManagerSlave(
      rpcEnv, rpcDriverEndpoint, executorId, SparkEnv.get.blockManager, null, sc.conf) {
      override def heartbeatMessages: Array[() => Heartbeat] = { Array(() => heartbeat) }
    }
  }
}
