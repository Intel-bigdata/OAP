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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.index.IndexUtils
import org.apache.spark.sql.execution.datasources.spinach.statistics._
import org.apache.spark.unsafe.Platform

class PartByValueStatisticsSuite extends StatisticsTest{
  // for all data in this suite, all internal rows appear only once
  // 1, 2, 3, ..., 300
  // `partNum` = 5, then the file content should be
  //    RowContent      curMaxIdx   curAccumulatorCount
  // (  1,  "test#1")       0              1
  // ( 61,  "test#61")     60             61
  // (121,  "test#121")   120            121
  // (181,  "test#181")   180            181
  // (241,  "test#241")   240            241
  // (300,  "test#300")   299            300

  test("test write function") {
  }

  test("read function test") {
  }

  test("read and write") {
  }
}
