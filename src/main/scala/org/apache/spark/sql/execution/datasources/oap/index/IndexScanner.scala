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
import org.apache.spark.sql.catalyst.expressions.{SortDirection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.io.OapIndexInfo
import org.apache.spark.sql.execution.datasources.oap.statistics.StaticsAnalysisResult
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


private[oap] object IndexScanner {
  val DUMMY_KEY_START: Key = new UnsafeRow() // we compare the ref not the value
  val DUMMY_KEY_END: Key = new UnsafeRow() // we compare the ref not the value
}

private[oap] abstract class IndexScanner(idxMeta: IndexMeta)
  extends Iterator[Int] with Serializable with Logging{

  // TODO Currently, only B+ tree supports indexs, so this flag is toggled only in
  // BPlusTreeScanner we can add other index-aware stats for other type of index later
  def canBeOptimizedByStatistics: Boolean = false

  var intervalArray: ArrayBuffer[RangeInterval] = _

  protected var keySchema: StructType = _

  def keyNames: Seq[String] = keySchema.fieldNames

  /**
   * Scan N items from each index entry.
   */
  private var _internalLimit : Int = 0

  // _internalLimit setter
  def internalLimit_= (scanNum : Int) : Unit = _internalLimit = scanNum

  // _internalLimit getter
  def internalLimit : Int = _internalLimit

  def indexEntryScanIsLimited() : Boolean = _internalLimit > 0

  def meta: IndexMeta = idxMeta

  def getSchema: StructType = keySchema

  def readBehavior(dataPath: Path, conf: Configuration): Double = {
    val indexPath = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    if (!indexPath.getFileSystem(conf).exists(indexPath)) {
      logDebug("No index file exist for data file: " + dataPath)
      StaticsAnalysisResult.FULL_SCAN
    } else {
      val start = System.currentTimeMillis()
      val enableOIndex = conf.getBoolean(OapConf.OAP_ENABLE_OINDEX.key,
        OapConf.OAP_ENABLE_OINDEX.defaultValue.get)
      var behavior: Double = StaticsAnalysisResult.FULL_SCAN
      val useIndex = enableOIndex && {
        behavior = readBehavior(indexPath, dataPath, conf)
        behavior != StaticsAnalysisResult.FULL_SCAN
      }
      val end = System.currentTimeMillis()
      logDebug("Index Selection Time (Executor): " + (end - start) + "ms")
      if (!useIndex) {
        logWarning("OAP index is skipped. Disable OAP_EXECUTOR_INDEX_SELECTION to use index.")
      } else {
        OapIndexInfo.partitionOapIndex.put(dataPath.toString, true)
        logInfo("Partition File " + dataPath.toString + " will use OAP index.\n")
      }
      behavior
    }
  }

  /**
   * Executor chooses to use index or not according to policies.
   *  1. OAP_EXECUTOR_INDEX_SELECTION is enabled.
   *  2. Statistics info recommends index scan.
   *  3. Considering about file I/O, index file size should be less
   *     than data file.
   *  4. TODO: add more.
   *
   * @param indexPath: index file path.
   * @param conf: configurations
   * @return Double to indicate if executor use index behavior,
    *         like FULL_SCAN, USE_INDEX or SKIP_INDEX
   */
  private def readBehavior(indexPath: Path, dataPath: Path, conf: Configuration): Double = {
    if (conf.getBoolean(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key,
      OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.defaultValue.get)) {
      // Index selection is enabled, executor chooses index according to policy.

      // Policy 1: index file size < data file size.
      val indexFileSize = indexPath.getFileSystem(conf).getContentSummary(indexPath).getLength
      val dataFileSize = dataPath.getFileSystem(conf).getContentSummary(dataPath).getLength
      val ratio = conf.getDouble(OapConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.key,
        OapConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.defaultValue.get)

      val filePolicyEnable =
        conf.getBoolean(OapConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.key,
        OapConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.defaultValue.get)
      if (filePolicyEnable && indexFileSize > dataFileSize * ratio) {
        return StaticsAnalysisResult.FULL_SCAN
      }

      val statsPolicyEnable =
        conf.getBoolean(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY.key,
          OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY.defaultValue.get)

      // Policy 2: statistics tells the scan cost
      if (statsPolicyEnable) {
        tryAnalyzeStatistics(indexPath, conf)
      } else {
        StaticsAnalysisResult.USE_INDEX
      }
      // More Policies
    } else {
      // Index selection is disabled, executor always uses index.
      StaticsAnalysisResult.USE_INDEX
    }
  }

  /**
   * Through getting statistics from related index file,
   * judging if we should bypass this datafile or full scan or by index.
   * return -1 means bypass, close to 1 means full scan and close to 0 means by index.
   * called before invoking [[initialize]].
   */
  private def tryAnalyzeStatistics(indexPath: Path, conf: Configuration): Double = {
    if (!canBeOptimizedByStatistics) {
      StaticsAnalysisResult.USE_INDEX
    } else if (intervalArray.isEmpty) {
      StaticsAnalysisResult.SKIP_INDEX
    } else {
      analyzeStatistics(indexPath, conf)
    }
  }

  protected def analyzeStatistics(indexPath: Path, conf: Configuration): Double = 0

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
  override def next(): Int = throw new NoSuchElementException("end of iterating.")
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

  def optimizeFilterBound(filter: Filter, ic: IndexContext): IntervalArrayMap = {
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
          .map(key => RangeInterval(key, key, includeStart = true, includeEnd = true))
          .to[ArrayBuffer]
        mutable.HashMap(attribute -> eqBounds)
      case EqualTo(attribute, ic(key)) =>
        val ranger = RangeInterval(key, key, includeStart = true, includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThanOrEqual(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            key,
            IndexScanner.DUMMY_KEY_END,
            includeStart = true,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThan(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            key,
            IndexScanner.DUMMY_KEY_END,
            includeStart = false,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThanOrEqual(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            key,
            includeStart = true,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThan(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            key,
            includeStart = true,
            includeEnd = false)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case IsNotNull(attribute) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            IndexScanner.DUMMY_KEY_END,
            includeStart = true,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case IsNull(attribute) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            IndexScanner.DUMMY_KEY_END,
            includeStart = true,
            includeEnd = true,
            isNull = true)
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

  def build(
      filters: Array[Filter],
      ic: IndexContext,
      scannerOptions: Map[String, String] = Map.empty,
      maxChooseSize: Int = 1): Array[Filter] = {
    if (filters == null || filters.isEmpty) return filters
    logDebug("Transform filters into Intervals:")
    val intervalMapArray = filters.map(optimizeFilterBound(_, ic))
    // reduce multiple hashMap to one hashMap("AND" operation)
    val intervalMap = intervalMapArray.reduce(
      (leftMap, rightMap) =>
        if (leftMap == null || leftMap.isEmpty) rightMap
        else if (rightMap == null || rightMap.isEmpty) leftMap
        else combineIntervalMaps(leftMap, rightMap, ic, needMerge = true)
    )

    if (intervalMap.nonEmpty) {
      intervalMap.foreach(intervals =>
        logDebug("\t" + intervals._1 + ": " + intervals._2.mkString(" - ")))

      ic.buildScanners(intervalMap, scannerOptions, maxChooseSize)
    }

    filters.filterNot(canSupport(_, ic))
  }

}

private[oap] class IndexScanners(val scanners: Seq[IndexScanner])
  extends Iterator[Int] with Serializable with Logging{

  private var actualUsedScanners: Seq[IndexScanner] = _

  private var backendIter: Iterator[Int] = _

  def indexIsAvailable(dataPath: Path, conf: Configuration): Boolean = {
    val scannersAndStatics = scanners
      .map(scanner => (scanner, scanner.readBehavior(dataPath, conf)))
      // _ is (scanner, StaticsAnalysisResult)
      .filter(_._2 != StaticsAnalysisResult.FULL_SCAN)
    scannersAndStatics.length match {
      case 0 => false
      case _ if scannersAndStatics.exists(_._2 == StaticsAnalysisResult.SKIP_INDEX) =>
        actualUsedScanners = Seq.empty
        true
      case _ => actualUsedScanners = scannersAndStatics.map(_._1)
        true
    }
  }

  def order: SortDirection = actualUsedScanners.head.meta.indexType.indexOrder.head

  def initialize(dataPath: Path, conf: Configuration): IndexScanners = {
    backendIter = actualUsedScanners.length match {
      case 0 => Iterator.empty
      case 1 =>
        actualUsedScanners.head.initialize(dataPath, conf)
        actualUsedScanners.head.toArray.iterator
      case _ =>
        actualUsedScanners.par.foreach(_.initialize(dataPath, conf))
        actualUsedScanners.map(_.toSet)
          .reduce((left, right) => {
            if (left.isEmpty || right.isEmpty) Set.empty
            else left.intersect(right)
          }).iterator
    }
    this
  }

  override def hasNext: Boolean = backendIter.hasNext

  override def next(): Int = backendIter.next

  override def toString(): String = scanners.map(_.toString()).mkString("|")

}

