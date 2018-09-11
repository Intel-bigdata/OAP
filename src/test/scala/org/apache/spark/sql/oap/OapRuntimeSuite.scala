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

import java.util.concurrent.{Executors, ExecutorService, TimeUnit}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.oap.SharedOapLocalClusterContext

class OapRuntimeSuite extends QueryTest with SharedOapLocalClusterContext {

  test("OapRuntime is created once") {
    val oapruntime = new Array[OapRuntime](2)
    val threadPool: ExecutorService = Executors.newFixedThreadPool(2)
    try {
      for (i <- 0 to 1) {
        threadPool.execute(new Runnable {
          override def run(): Unit = {
            oapruntime(i) = OapRuntime.getOrCreate
          }
        })
      }
      threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)
    } finally {
      threadPool.shutdown()
    }
    assert(oapruntime(0) == oapruntime(1))
  }

  test("get sparkSession from OapRuntime") {
    assert(OapRuntime.getOrCreate.sparkSession == spark)
  }
}

