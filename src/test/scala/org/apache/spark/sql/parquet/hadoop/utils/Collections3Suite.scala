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
package org.apache.spark.sql.parquet.hadoop.utils

import com.google.common.collect.{Maps, Sets}
import org.apache.parquet.hadoop.utils.Collections3

import org.apache.spark.SparkFunSuite

class Collections3Suite extends SparkFunSuite {

  test("toSetMultiMap") {
    val map = Maps.newHashMap[Int, Int]()
    map.put(1, 1)
    map.put(2, 2)
    map.put(3, 3)

    val ret = Collections3.toSetMultiMap(map)
    assert(ret.size() == 3)
    assert(ret.get(1).equals(Sets.newHashSet(1)))
    assert(ret.get(2).equals(Sets.newHashSet(2)))
    assert(ret.get(3).equals(Sets.newHashSet(3)))
  }
}
