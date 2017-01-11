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

import org.apache.spark.util.collection.BitSet
import scala.util.hashing.{MurmurHash3 => MH3}

/**
 * Implementation for Bloom filter.
 */
class BloomFilter(maxBitCount: Int, numOfHashFunc: Int) {
  private val bloomBitSet: BitSet = new BitSet(maxBitCount)
  private val hashFunctions: Array[BloomHashFunction] =
    BloomHashFunction.getMurmurHashFunction(maxBitCount, numOfHashFunc)

  def this() = this(1 << 6, 3)

  private def getIndices(value: String): Array[Int] =
    hashFunctions.map(func => func.hash(value))

  def addValue(value: String): Unit = {
    val indices = getIndices(value)
    indices.foreach(bloomBitSet.set)
  }

  def checkExist(value: String): Boolean = {
    val indices = getIndices(value)
    for (i <- indices)
      if (!bloomBitSet.get(i)) return false
    true
  }
}

trait BloomHashFunction {
  def hash(value: String): Int
}

class MurmurHashFunction(maxCount: Int, seed: Int) extends BloomHashFunction {
  def hash(value: String): Int =
    (MH3.stringHash(value, seed) % maxCount + maxCount) % maxCount
}

object BloomHashFunction {
  def getMurmurHashFunction(maxCount: Int, cnt: Int): Array[BloomHashFunction] = {
    (0 until cnt).map(i => new MurmurHashFunction(maxCount, i.toString.hashCode())).toArray
  }
}

