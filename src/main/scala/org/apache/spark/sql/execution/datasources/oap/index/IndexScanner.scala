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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


private[oap] object IndexScanner {
  val DUMMY_KEY_START: Key = new UnsafeRow() // we compare the ref not the value
  val DUMMY_KEY_END: Key = new UnsafeRow() // we compare the ref not the value
}

private[oap] abstract class IndexScanner(idxMeta: IndexMeta)
  extends Iterator[Long] with Serializable with Logging{

  // TODO Currently, only B+ tree supports indexs, so this flag is toggled only in
  // BPlusTreeScanner we can add other index-aware stats for other type of index later
  def canBeOptimizedByStatistics: Boolean = false

  @transient protected var ordering: Ordering[Key] = _
  var intervalArray: ArrayBuffer[RangeInterval] = _
  protected var keySchema: StructType = _

  /**
   * Scan N items from each index entry.
   */
  private var limitScan : Int = 0

  def setScanNumLimit(scanNum : Int) : Unit = {
    limitScan = scanNum
  }

  def getLimitScanNum() : Int = limitScan

  def limitScanEnabled() : Boolean = limitScan > 0

  def meta: IndexMeta = idxMeta
  def getSchema: StructType = keySchema

  def existRelatedIndexFile(dataPath: Path, conf: Configuration): Boolean = {
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    path.getFileSystem(conf).exists(path)
  }

  def withKeySchema(schema: StructType): IndexScanner = {
    this.keySchema = schema
    this
  }

  def initialize(dataPath: Path, conf: Configuration): IndexScanner
}

// A dummy scanner will actually not do any scanning
private[oap] object DUMMY_SCANNER extends IndexScanner(null) {
  override def initialize(path: Path, configuration: Configuration): IndexScanner = { this }
  override def hasNext: Boolean = false
  override def next(): Long = throw new NoSuchElementException("end of iterating.")
  override def meta: IndexMeta = throw new NotImplementedError()
}

// The building of Search Scanner according to the filter and indices,
private[oap] object ScannerBuilder extends Logging {

  type IntervalArrayMap = mutable.HashMap[String, ArrayBuffer[RangeInterval]]

  def combineIntervalMaps(leftMap: IntervalArrayMap,
                          rightMap: IntervalArrayMap,
                          ic: IndexContext,
                          needMerge: Boolean): IntervalArrayMap = {

    for ((attribute, intervals) <- rightMap) {
      if (leftMap.contains(attribute)) {
        attribute match {
          case ic (filterOptimizer) => // extract the corresponding scannerBuilder
            // combine all intervals of the same attribute of leftMap and rightMap
            if (needMerge) leftMap.put(attribute,
              filterOptimizer.mergeBound(leftMap.getOrElseUpdate (attribute, null), intervals) )
            // add bound of the same attribute to the left map
            else leftMap.put(attribute,
              filterOptimizer.addBound(leftMap.getOrElse (attribute, null), intervals) )
          case _ => // this attribute does not exist, do nothing
        }
      }
      else {
        leftMap.put(attribute, intervals)
      }
    }
    leftMap
  }

  def optimizeFilterBound(filter: Filter, ic: IndexContext)
  : mutable.HashMap[String, ArrayBuffer[RangeInterval]] = {
    filter match {
      case And(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        combineIntervalMaps(leftMap, rightMap, ic, needMerge = true)
      case Or(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        combineIntervalMaps(leftMap, rightMap, ic, needMerge = false)
      case In(attribute, ic(keys)) =>
        val eqBounds = keys.distinct
          .map(key => new RangeInterval(key, key, true, true))
          .to[ArrayBuffer]
        scala.collection.mutable.HashMap(attribute -> eqBounds)
      case EqualTo(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, key, true, true)
        scala.collection.mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, IndexScanner.DUMMY_KEY_END, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, IndexScanner.DUMMY_KEY_END, false, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(IndexScanner.DUMMY_KEY_START, key, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(IndexScanner.DUMMY_KEY_START, key, true, false)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case _ => mutable.HashMap.empty
    }
  }

  // return whether a Filter predicate can be supported by our current work
  def canSupport(filter: Filter, ic: IndexContext): Boolean = {
    filter match {
      case EqualTo(ic(indexer), _) => true
      case GreaterThan(ic(indexer), _) => true
      case GreaterThanOrEqual(ic(indexer), _) => true
      case LessThan(ic(indexer), _) => true
      case LessThanOrEqual(ic(indexer), _) => true
      case Or(ic(indexer), _) => true
      case And(ic(indexer), _) => true
      case In(ic(indexer), _) => true
      case _ => false
    }
  }

  def build(filters: Array[Filter], ic: IndexContext): Array[Filter] = {
    if (filters == null || filters.isEmpty) return filters
    logDebug("Transform filters into Intervals:")
    val intervalMapArray = filters.map(optimizeFilterBound(_, ic))
    // reduce multiple hashMap to one hashMap(AND operation)
    val intervalMap = intervalMapArray.reduce(
      (leftMap, rightMap) =>
        if (leftMap == null || leftMap.isEmpty) rightMap
        else if (rightMap == null || rightMap.isEmpty) leftMap
        else combineIntervalMaps(leftMap, rightMap, ic, needMerge = true)
    )

    if (intervalMap != null) {
      intervalMap.foreach(intervals =>
        logDebug("\t" + intervals._1 + ": " + intervals._2.mkString(" - ")))

      ic.selectAvailableIndex(intervalMap)
      val (num, idxMeta) = ic.getBestIndexer(intervalMap.size)
      ic.buildScanner(num, idxMeta, intervalMap)
    }

    filters.filterNot(canSupport(_, ic))
  }

}
