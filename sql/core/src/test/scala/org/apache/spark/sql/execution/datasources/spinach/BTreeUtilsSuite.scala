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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.spinach.utils.BTreeUtils


class BTreeUtilsSuite extends SparkFunSuite with Logging {
  test("height") {
    assert(BTreeUtils.height(25) == 2)
    assert(BTreeUtils.height(26) == 3)
    assert(BTreeUtils.height(125) == 3)
    assert(BTreeUtils.height(126) == 3)
    assert(BTreeUtils.height(1000000) == 3)
  }

  test("total number") {
    assert(BTreeUtils.generate(126).sum == 126)
    assert(BTreeUtils.generate(1000000).sum == 1000000)
  }

  test("generate b tree") {
    assert(BTreeUtils.generate2(6).toString == "[2 3 3]")
    assert(BTreeUtils.generate2(11).toString == "[3 4 4 3]")
    assert(BTreeUtils.generate2(26).toString == "[2 [3 5 4 4] [3 5 4 4]]")
    assert(BTreeUtils.generate2(1000).toString == "[10 [10 10 10 10 10 10 10 10 10 10 10]" +
      " [10 10 10 10 10 10 10 10 10 10 10] [10 10 10 10 10 10 10 10 10 10 10] [10 10 10 10" +
      " 10 10 10 10 10 10 10] [10 10 10 10 10 10 10 10 10 10 10] [10 10 10 10 10 10 10 10 " +
      "10 10 10] [10 10 10 10 10 10 10 10 10 10 10] [10 10 10 10 10 10 10 10 10 10 10] [10" +
      " 10 10 10 10 10 10 10 10 10 10] [10 10 10 10 10 10 10 10 10 10 10]]")
  }
}
