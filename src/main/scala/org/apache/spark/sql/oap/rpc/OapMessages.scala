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

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerId

private[spark] object OapMessages {

  sealed trait OapMessage extends Serializable

  sealed trait ToOapRpcManagerSlave extends OapMessage
  sealed trait ToOapRpcManagerMaster extends OapMessage
  sealed trait Heartbeat extends ToOapRpcManagerMaster

  /* Two-way messages, this is just for test */
  case class DummyMessage(id: String, someContent: String) extends
    ToOapRpcManagerSlave with ToOapRpcManagerMaster

  /* Master to slave messages */
  case class CacheDrop(indexName: String) extends ToOapRpcManagerSlave

  /* Slave to master messages */
  case class RegisterOapRpcManager(
      executorId: String, oapRpcManagerEndpoint: RpcEndpointRef) extends ToOapRpcManagerMaster
  case class DummyHeartbeat(someContent: String) extends Heartbeat
  case class FiberCacheHeartbeat(
      executorId: String, blockManagerId: BlockManagerId, content: String) extends Heartbeat
  case class IndexHeartbeat(executorId: String, blockManagerId: BlockManagerId, content: String)
    extends Heartbeat
  case class FiberCacheMetricsHeartbeat(
      executorId: String, blockManagerId: BlockManagerId, content: String) extends Heartbeat
}
