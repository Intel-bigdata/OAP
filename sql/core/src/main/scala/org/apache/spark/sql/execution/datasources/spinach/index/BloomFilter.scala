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

package org.apache.spark.sql.execution.datasources.spinach.index

import scala.collection.mutable
import scala.util.hashing.{MurmurHash3 => MH3}

/**
 * Implementation for Bloom filter.
 */
class BloomFilter(maxBitCount: Int, numOfHashFunc: Int)
                 (var bloomBitSet: mutable.BitSet = null) {
//  private var bloomBitSet: mutable.BitSet = new mutable.BitSet(maxBitCount)
  private val hashFunctions: Array[BloomHashFunction] =
    BloomHashFunction.getMurmurHashFunction(maxBitCount, numOfHashFunc)
  if (bloomBitSet == null) {
    bloomBitSet = new mutable.BitSet(maxBitCount)
  }

  def this() = this(1 << 16, 3)()

  def getBitMapLongArray: Array[Long] = bloomBitSet.toBitMask
  def getNumOfHashFunc: Int = numOfHashFunc

  private def getIndices(value: String): Array[Int] =
    hashFunctions.map(func => func.hash(value))
  private def getIndices(value: Array[Byte]): Array[Int] =
    hashFunctions.map(func => func.hash(value))

  def addValue(value: String): Unit = {
    val indices = getIndices(value)
    indices.foreach(bloomBitSet.add)
  }

  def checkExist(value: String): Boolean = {
    val indices = getIndices(value)
    for (i <- indices)
      if (!bloomBitSet.contains(i)) return false
    true
  }

  def addValue(data: Array[Byte]): Unit = {
    val indices = getIndices(data)
    indices.foreach(bloomBitSet.add)
  }

  def checkExist(data: Array[Byte]): Boolean = {
    val indices = getIndices(data)
    for (i <- indices)
      if (!bloomBitSet.contains(i)) return false
    true
  }
}

object BloomFilter {
  def apply(longArr: Array[Long], numOfHashFunc: Int): BloomFilter =
    new BloomFilter(longArr.length * 64, numOfHashFunc)(mutable.BitSet.fromBitMask(longArr))
}

private[spinach] trait BloomHashFunction {
  def hash(value: String): Int
  def hash(value: Array[Byte]): Int
}

private[spinach] class MurmurHashFunction(maxCount: Int, seed: Int) extends BloomHashFunction {
  def hash(value: String): Int =
    (MH3.stringHash(value, seed) % maxCount + maxCount) % maxCount

  override def hash(value: Array[Byte]): Int =
    (MH3.bytesHash(value, seed) % maxCount + maxCount) % maxCount
}

private[spinach] object BloomHashFunction {
  def getMurmurHashFunction(maxCount: Int, cnt: Int): Array[BloomHashFunction] = {
    (0 until cnt).map(i => new MurmurHashFunction(maxCount, i.toString.hashCode())).toArray
  }
}

