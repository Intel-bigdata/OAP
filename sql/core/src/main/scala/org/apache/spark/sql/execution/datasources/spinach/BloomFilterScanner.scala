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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.unsafe.Platform

private[spinach] case class BloomFilterScanner(me: IndexMeta) extends IndexScanner(me) {
  var stopFlag: Boolean = _

  var bloomFilter: BloomFilter = _

  var numOfElem: Int = _

  var curIdx: Int = _

  override def hasNext: Boolean = !stopFlag && curIdx < numOfElem

  override def next(): Long = {
    val tmp = curIdx
    curIdx += 1
    tmp.toLong
  }

  lazy val equalValues: Array[Key] = { // get equal value from intervalArray
    if (intervalArray.nonEmpty) {
      // should not use ordering.compare here
      intervalArray.filter(interval => (interval.start eq interval.end)
        && interval.startInclude && interval.endInclude).map(_.start).toArray
    } else null
  }

  override def initialize(inputPath: Path, configuration: Configuration): IndexScanner = {
    assert(keySchema ne null)
    this.ordering = GenerateOrdering.create(keySchema)

    val path = IndexUtils.indexFileFromDataFile(inputPath, meta.name)
    val indexScanner = IndexFiber(IndexFile(path))
    val indexData: IndexFiberCacheData = FiberCacheManager(indexScanner, configuration)

    def buffer: DataFiberCache = DataFiberCache(indexData.fiberData)
    def getBaseObj = buffer.fiberData.getBaseObject
    def getBaseOffset = buffer.fiberData.getBaseOffset
    val bitArrayLength = Platform.getInt(getBaseObj, getBaseOffset + 0 )
    val numOfHashFunc = Platform.getInt(getBaseObj, getBaseOffset + 4)
    numOfElem = Platform.getInt(getBaseObj, getBaseOffset + 8)

    var cur_pos = 4
    val bitSetLongArr = (0 until bitArrayLength).map( i => {
      cur_pos += 8
      Platform.getLong(getBaseObj, getBaseOffset + cur_pos)
    }).toArray

    bloomFilter = BloomFilter(bitSetLongArr, numOfHashFunc)

    val projector = UnsafeProjection.create(keySchema)

    // TODO need optimization while considering multi-column
    stopFlag = if (equalValues != null && equalValues.length > 0) {
      !equalValues.map(value => bloomFilter
        .checkExist(projector(value).getBytes))
        .reduceOption(_ || _).getOrElse(false)
    } else false
    curIdx = 0
    this
  }

  override def toString: String = "BloomFilterScanner"
}
