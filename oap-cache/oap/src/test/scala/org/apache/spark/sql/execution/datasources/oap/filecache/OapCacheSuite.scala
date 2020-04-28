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

package org.apache.spark.sql.execution.datasources.oap.filecache

import scala.language.postfixOps
import scala.sys.process._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.test.oap.SharedOapContext

class OapCacheSuite extends SharedOapContext with Logging{

  override def beforeAll(): Unit = super.beforeAll()

  override def afterAll(): Unit = super.afterAll()

  test("detectPM") {
    val notFound = "Command 'ipmctl' not found."
    val noDIMMs = "No DIMMs in the system."
    OapCache.detectAEPRes = "sudo ipmctl show -dimm".!!
    assert(OapCache.detectPM() == true)
    OapCache.detectAEPRes = notFound
    assert(OapCache.detectPM() == false)
    OapCache.detectAEPRes = noDIMMs
    assert(OapCache.detectPM() == false)
  }
}
