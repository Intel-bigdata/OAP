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

/**
 * Bloom filter test suit
 */
class BloomFilterSuit extends SparkFunSuite {
  val strs = Array("bloom", "fiter", "spark", "fun", "suite")
  val bloomFilter = new BloomFilter()

  for (s <- strs) {
    bloomFilter.addValue(s)
  }

  test("exist test") {
    assert(!bloomFilter.checkExist("gefei"), "gefei is not in this filter")
    assert(!bloomFilter.checkExist("sutie"), "sutie is not in this filter")
  }
}
