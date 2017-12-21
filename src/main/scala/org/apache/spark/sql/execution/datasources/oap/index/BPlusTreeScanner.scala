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

package org.apache.spark.sql.execution.datasources.oap.index

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.index.BTreeIndexRecordReader.BTreeFooter
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager

// we scan the index from the smallest to the largest,
// this will scan the B+ Tree (index) leaf node.
private[oap] class BPlusTreeScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {
  override def canBeOptimizedByStatistics: Boolean = true
  override def toString(): String = "BPlusTreeScanner"
  @transient protected var currentKeyArray: Array[CurrentKey] = _

  var currentKeyIdx = 0
  var recordReader: BTreeIndexRecordReader = _

  def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    logDebug("Loading Index File: " + path)
    logDebug("\tFile Size: " + path.getFileSystem(conf).getFileStatus(path).getLen)

    recordReader = BTreeIndexRecordReader(conf, keySchema)
    recordReader.initialize(path, intervalArray)
    this
  }

  override protected def analyzeStatistics(indexPath: Path, conf: Configuration): Double = {
    // TODO decouple with btreeindexrecordreader
    // This is called before the scanner call `initialize`
    val reader = BTreeIndexFileReader(conf, indexPath)
    val footerFiber = BTreeFiber(
      () => reader.readFooter(), reader.file.toString, reader.footerSectionId, 0)
    val footerCache = FiberCacheManager.get(footerFiber, conf)
    val footer = BTreeFooter(footerCache, keySchema)
    val offset = footer.getStatsOffset

    val stats = StatisticsManager.read(footerCache, offset, keySchema)
    StatisticsManager.analyse(stats, intervalArray, conf)
  }

  override def hasNext: Boolean = recordReader.hasNext

  override def next(): Int = recordReader.next()
}
