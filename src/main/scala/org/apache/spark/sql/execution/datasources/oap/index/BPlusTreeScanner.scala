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
import org.apache.spark.sql.execution.datasources.oap.statistics.StatsAnalysisResult

// we scan the index from the smallest to the largest,
// this will scan the B+ Tree (index) leaf node.
private[oap] class BPlusTreeScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {
  override def toString(): String = "BPlusTreeScanner"

  @transient var recordReader: BTreeIndexRecordReader = _

  // Set by analyzeStatistics()
  private var _totalRows: Long = 0
  override def totalRows(): Long = _totalRows

  def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    val indexPath = IndexUtils.getIndexFilePath(
      conf, dataPath, meta.name, meta.time)
    logDebug("Loading Index File: " + indexPath)
    logDebug("\tFile Size: " + indexPath.getFileSystem(conf).getFileStatus(indexPath).getLen)

    recordReader = BTreeIndexRecordReader(conf, keySchema, indexPath)
    recordReader.initialize(indexPath, intervalArray)

    // For some case, analyzeStatistics will be skipped, so we have to get totalRows here as well.
    _totalRows = recordReader.totalRows()
    this
  }

  override protected def analyzeStatistics(
      indexPath: Path,
      conf: Configuration): StatsAnalysisResult = {
    var recordReader = BTreeIndexRecordReader(conf, keySchema, indexPath)
    try {
      val result = recordReader.analyzeStatistics(keySchema, intervalArray)
      _totalRows = recordReader.totalRows()
      result
    } finally {
      if (recordReader != null) {
        recordReader.close()
        recordReader = null
      }
    }
  }

  override def hasNext: Boolean = recordReader.hasNext

  override def next(): Int = recordReader.next()
}
