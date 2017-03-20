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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


private[spinach] object RangeScanner {
  val DUMMY_KEY_START: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
  val DUMMY_KEY_END: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
}

private[spinach] abstract class RangeScanner(idxMeta: IndexMeta)
  extends Iterator[Long] with Serializable with Logging {
  @transient protected var ordering: Ordering[Key] = _
  var intervalArray: ArrayBuffer[RangeInterval] = _
  protected var keySchema: StructType = _

  def meta: IndexMeta = idxMeta

  def existRelatedIndexFile(dataPath: Path, conf: Configuration): Boolean = {
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name)
    path.getFileSystem(conf).exists(path)
  }

  def withKeySchema(schema: StructType): RangeScanner = {
    this.keySchema = schema
    this
  }

  def initialize(dataPath: Path, conf: Configuration): RangeScanner
}

// A dummy scanner will actually not do any scanning
private[spinach] object DUMMY_SCANNER extends RangeScanner(null) {
  //  override def shouldStop(key: CurrentKey): Boolean = true
//  override def intervalShouldStop(i: Int): Boolean = true
  override def initialize(path: Path, configuration: Configuration): RangeScanner = { this }
  override def hasNext: Boolean = false
  override def next(): Long = throw new NoSuchElementException("end of iterating.")
  //  override def withNewStart(key: Key, include: Boolean): RangeScanner = this
  //  override def withNewEnd(key: Key, include: Boolean): RangeScanner = this
  override def meta: IndexMeta = throw new NotImplementedError()
  //  override def start: Key = throw new NotImplementedError()
}

// The building of Search Scanner according to the filter and indices,
private[spinach] object ScannerBuilder extends Logging {
  def optimizeFilterBound(filter: Filter, ic: IndexContext)
  : mutable.HashMap[String, ArrayBuffer[RangeInterval]] = {
    filter match {
      case And(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        for((attribute, intervals) <- rightMap) {
          if (leftMap.contains(attribute)) {
            attribute match {
              case ic (filterOptimizer) => // extract the corresponding scannerBuilder
                // combine all intervals of the same attribute of leftMap and rightMap
                leftMap.put(attribute,
                  filterOptimizer.mergeBound(leftMap.getOrElseUpdate (attribute, null), intervals) )
              case _ => // this attribute does not exist, do nothing
            }
          }
          else {
            leftMap.put(attribute, intervals)
          }
        }// end for
        // rightMap.clear()
        leftMap
      case Or(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        for((attribute, intervals) <- rightMap) {
          if (leftMap.contains(attribute)) {
            attribute match {
              case ic (filterOptimizer) => // extract the corresponding scannerBuilder
                // add bound of the same attribute to the left map
                leftMap.put(attribute,
                  filterOptimizer.addBound(leftMap.getOrElse (attribute, null), intervals) )
              case _ => // this attribute does not exist, do nothing
            }
          }
          else {
            leftMap.put(attribute, intervals)
          }

        }// end for
        //        rightMap.clear()
        leftMap

      case EqualTo(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, key, true, true)
        scala.collection.mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, RangeScanner.DUMMY_KEY_END, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, RangeScanner.DUMMY_KEY_END, false, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(RangeScanner.DUMMY_KEY_START, key, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(RangeScanner.DUMMY_KEY_START, key, true, false)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case _ => null
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
      case _ => false
    }
  }

  def build(filters: Array[Filter], ic: IndexContext): Array[Filter] = {
    if (filters == null || filters.isEmpty) return filters
    logDebug("Transform filters into Intervals:")
    val intervalMapArray = filters.map(optimizeFilterBound(_, ic))
    // reduce multiple hashMap to one hashMap(AND operation)
    val intervalMap = intervalMapArray.reduce(
      (leftMap, rightMap) => {
        if (leftMap == null || leftMap.isEmpty) {
          rightMap
        }
        else if (rightMap == null || rightMap.isEmpty) {
          leftMap
        }
        else {
          for ((attribute, intervals) <- rightMap) {
            if (leftMap.contains(attribute)) {
              attribute match {
                case ic (filterOptimizer) => // extract the corresponding scannerBuilder
                  // combine all intervals of the same attribute of leftMap and rightMap
                  leftMap.put(attribute,
                    filterOptimizer.mergeBound(leftMap(attribute), intervals) )
                case _ => // this attribute is not index, do nothing
              }
            }
            else {
              leftMap.put(attribute, intervals)
            }
          } // end for
          // rightMap.clear()
          leftMap
        }
      }
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
