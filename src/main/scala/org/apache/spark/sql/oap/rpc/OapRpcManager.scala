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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.oap.rpc.OapMessages.OapMessage

/**
 * A base trait of OapRpcManagerMaster/Slave, use this class running on Driver/Executors to send
 * messages to Executors/Driver.
 * Note that not all functions need to be implemented in OapRpcManagerMaster/Slave
 */

trait OapRpcManager extends Logging {

  private[spark] def send(message: OapMessage): Unit

  private[spark] def stop(): Unit = {}

}
