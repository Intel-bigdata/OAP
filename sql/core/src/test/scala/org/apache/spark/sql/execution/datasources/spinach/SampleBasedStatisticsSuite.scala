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
package org.apache.spark.sql.execution.datasources.spinach

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.execution.datasources.spinach.index.IndexUtils
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.unsafe.Platform


class SampleBasedStatisticsSuite extends StatisticsTest{

  class TestSampleBaseStatistics(rate: Double = 0.1) extends SampleBasedStatistics {
    override def takeSample(keys: ArrayBuffer[Key], size: Int): Array[Key] = keys.take(size).toArray
  }

  test("test write function") {
  }

  test("read function test") {
  }

  test("read and write") {
  }
}

