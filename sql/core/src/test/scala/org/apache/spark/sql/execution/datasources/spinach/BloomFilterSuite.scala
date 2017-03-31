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
import org.apache.spark.sql.execution.datasources.spinach.index.BloomFilter


/**
 * Bloom filter test suit
 */
class BloomFilterSuite extends SparkFunSuite {
  val strs = Array("bloom", "filter", "spark", "fun", "suite")
  val bloomFilter = new BloomFilter()

  for (s <- strs) {
    bloomFilter.addValue(s)
  }

  test("exist test") {
    assert(!bloomFilter.checkExist("gefei"), "gefei is not in this filter")
    assert(!bloomFilter.checkExist("sutie"), "sutie is not in this filter")
    assert(bloomFilter.checkExist("bloom"), "bloom is in this filter")
    assert(bloomFilter.checkExist("filter"), "filter is in this filter")
    assert(bloomFilter.checkExist("spark"), "spark is in this filter")
    assert(bloomFilter.checkExist("fun"), "fun is in this filter")
    assert(bloomFilter.checkExist("suite"), "suite is in this filter")
  }

  test("filter rebuild test") {
    val numOfHashFunc = bloomFilter.getNumOfHashFunc
    val longArr = bloomFilter.getBitMapLongArray

    val new_bf = BloomFilter(longArr, numOfHashFunc)
    assert(!new_bf.checkExist("gefei"), "gefei is not in this filter")
    assert(!new_bf.checkExist("sutie"), "sutie is not in this filter")
    assert(new_bf.checkExist("bloom"), "bloom is in this filter")
    assert(new_bf.checkExist("filter"), "filter is in this filter")
    assert(new_bf.checkExist("spark"), "spark is in this filter")
    assert(new_bf.checkExist("fun"), "fun is in this filter")
    assert(new_bf.checkExist("suite"), "suite is in this filter")
  }
}
