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

package org.apache.spark.sql.execution.datasources.oap

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCacheManager, MemoryManager}

class OapEnv(
    val memoryManager: MemoryManager,
    val fiberCacheManager: FiberCacheManager
) extends Logging {

  // TODO: for cleanup
  def stop(): Unit = {
    fiberCacheManager.stop()
    memoryManager.stop()
  }
}

object OapEnv {
  private var env: OapEnv = create()
  def get: OapEnv = env

  private def set(e: OapEnv): Unit = env = e

  private def create(): OapEnv = {
    val sparkEnv = SparkEnv.get
    if (sparkEnv == null) throw new OapException("Can't run OAP without SparkContext")

    val memoryManager = new MemoryManager(sparkEnv.memoryManager, sparkEnv.conf)
    val fiberCacheManager = new FiberCacheManager(memoryManager.maxMemory)
    new OapEnv(memoryManager, fiberCacheManager)
  }

  /**
   * Create a new OapEnv with latest SparkConf. For test purpose
   */
  def update(): Unit = {
    // TODO: clean previous OapEnv
    if (env != null) {
      env.stop()
      env = null
    }
    val oapEnv = create()
    set(oapEnv)
  }
}
