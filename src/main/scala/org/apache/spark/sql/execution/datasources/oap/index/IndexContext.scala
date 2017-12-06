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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.types.StructType


private[oap] class IndexContext(meta: DataSourceMeta) extends Logging {
  // availableIndexes keeps the available indexes for the current SQL query statement
  // (Int, IndexMeta):
  // if indexType is BloomFilter, then the Int represents the indice of the Index entries;
  // if indexType is B+Tree and single column,
  // then the Int represents the indice of the Index entries, that is 0;
  // if indexType is B+Tree and multi-column,
  // then the Int represents the last matched column indice of the Index entries
  private val availableIndexes = new mutable.ArrayBuffer[(Int, IndexMeta)]()
  private val filterMap = new mutable.HashMap[String, FilterOptimizer]()
  private var scanner: IndexScanner = _

  def getScanner: Option[IndexScanner] = Option(scanner)

  /**
   * clear the available indexes and filter info, reset index scanner
   */
  def clear(): Unit = {
    availableIndexes.clear()
    filterMap.clear()
    scanner = null
  }

  private def selectAvailableIndex(intervalMap: mutable.HashMap[String, ArrayBuffer[RangeInterval]])
  : Unit = {
    logDebug("Selecting Available Index:")
    var idx = 0
    while (idx < meta.indexMetas.length) {
      meta.indexMetas(idx).indexType match {
        case BTreeIndex(entries) if entries.length == 1 =>
          val attribute = meta.schema(entries(0).ordinal).name
          if (intervalMap.contains(attribute)) {
            availableIndexes.append((0, meta.indexMetas(idx)) )
          }
        case BTreeIndex(entries) =>
          var num = 0 // the number of matched column
          var flag = 0
          // flag (terminated indication):
          // 0 -> Equivalence column; 1 -> Range column; 2 -> Absent column
          for (entry <- entries if flag == 0) {
            val attribute = meta.schema(entry.ordinal).name
            if (intervalMap.contains(attribute) && intervalMap(attribute).length == 1) {
              val start = intervalMap(attribute).head.start
              val end = intervalMap(attribute).head.end
              val ordering = unapply(attribute).get.order
              if(start != IndexScanner.DUMMY_KEY_START &&
                end != IndexScanner.DUMMY_KEY_END &&
                ordering.compare(start, end) == 0) {num += 1} else flag = 1
            }
            else {
              if (!intervalMap.contains(attribute)) flag = 2 else flag = 1
            }
          } // end for
          if (flag == 1) num += 1
          if (num>0) {
            availableIndexes.append( (num-1, meta.indexMetas(idx)) )
          }
        case BitMapIndex(entries) =>
          for (entry <- entries) {
            if (intervalMap.contains(meta.schema(entry).name)) {
              availableIndexes.append((entries.indexOf(entry), meta.indexMetas(idx)) )
            }
          }
        case other => // TODO support other types of index
      }
      idx += 1
    } // end while
    availableIndexes.foreach(indices =>
      logDebug("\t" + indices._2.toString + "; lastIdx: " + indices._1))
  }

  /**
   * A simple approach to select best indexer:
   * For B+ tree index, we expect to make full use of index:
   * On one hand, match as many attributes as possible in a SQL statement;
   * On the other hand, use as many attributes as possible in a B+ tree index
   * So we want the number of matched attributes to be close to
   * both the total number of attributes in a SQL statement
   * and the total number of entries in a B+ tree index candidate
   * we introduce a variable ratio to indicate the match extent
   * ratio = totalAttributes/matchedAttributed + totalIndexEntries/matchedAttributes
   * @param attrNum: the total number of attributes in the SQL statement
   * @return (Int, IndexMeta): the best indexMeta,
   *         and the Int is the index of the last matched attribute in the index entries
   */
  def getBestIndexer(attrNum: Int): (Int, IndexMeta) = {
    var lastIdx = -1
    var bestIndexer: IndexMeta = null
    var ratio: Double = 0.0
    var isFirst = true
    logDebug("Get Best Index:")
    for ((idx, indexMeta) <- availableIndexes) {
      indexMeta.indexType match {
        case BTreeIndex(entries) =>
          val matchedAttr: Double = idx + 1
          val currentRatio = attrNum/matchedAttr + entries.length/matchedAttr
          if (isFirst || ratio > currentRatio) {
            ratio = currentRatio
            bestIndexer = indexMeta
            lastIdx = idx
            isFirst = false
          }
        case _ =>
      }
    }
    if (bestIndexer == null && availableIndexes.nonEmpty) {
      lastIdx = availableIndexes.head._1
      bestIndexer = availableIndexes.head._2
    }
    if (bestIndexer != null) {
      logDebug("\t" + bestIndexer.toString + "; lastIdx: " + lastIdx)
    } else {
      logDebug("\t" + "No best indexer is found.")
    }
    (lastIdx, bestIndexer)
  }

  def buildScanner(
      intervalMap: mutable.HashMap[String, ArrayBuffer[RangeInterval]],
      options: Map[String, String] = Map.empty): Unit = {
    selectAvailableIndex(intervalMap)
    val (lastIdx, bestIndexer) = getBestIndexer(intervalMap.size)

    //    intervalArray.sortWith(compare)
    logDebug("Building Index Scanner with IndexMeta and IntervalMap ...")

    if (lastIdx == -1 && bestIndexer == null) return
    var keySchema: StructType = null
    bestIndexer.indexType match {
      case BTreeIndex(entries) if entries.length == 1 =>
        keySchema = new StructType().add(meta.schema(entries(lastIdx).ordinal))
        scanner = new BPlusTreeScanner(bestIndexer)
        val attribute = meta.schema(entries(lastIdx).ordinal).name
        val filterOptimizer = unapply(attribute).get
        scanner.intervalArray =
          intervalMap(attribute).sortWith(filterOptimizer.compareRangeInterval)
      case BTreeIndex(entries) =>
        val indexFields = for (idx <- entries.map(_.ordinal)) yield meta.schema(idx)
        val fields = indexFields.slice(0, lastIdx + 1)
        keySchema = StructType(fields)
        scanner = new BPlusTreeScanner(bestIndexer)
        val attributes = fields.map(_.name) // get column names in the composite index
        scanner.intervalArray = new ArrayBuffer[RangeInterval](intervalMap(attributes.last).length)

        for (i <- intervalMap(attributes.last).indices) {
          val startKeys = attributes.indices.map(attrIdx =>
            if (attrIdx == attributes.length-1) intervalMap(attributes(attrIdx))(i).start
            else intervalMap(attributes(attrIdx)).head.start )
          val compositeStartKey = startKeys.reduce((key1, key2) => new JoinedRow(key1, key2))

          val endKeys = attributes.indices.map(attrIdx =>
            if (attrIdx == attributes.length-1) intervalMap(attributes(attrIdx))(i).end
            else intervalMap(attributes(attrIdx)).head.end )
          val compositeEndKey = endKeys.reduce((key1, key2) => new JoinedRow(key1, key2))

          scanner.intervalArray.append(
            RangeInterval(compositeStartKey, compositeEndKey,
              intervalMap(attributes.last)(i).startInclude,
              intervalMap(attributes.last)(i).endInclude)
          )

        } // end for
      case BitMapIndex(entries) =>
        val attribute = meta.schema(entries(lastIdx)).name
        val filterOptimizer = unapply(attribute).get
        val sortedIntervalArray =
          intervalMap(attribute).sortWith(filterOptimizer.compareRangeInterval)
        val singleValueIntervalArray =
          sortedIntervalArray.filter(filterOptimizer.isSingleValueInterval)
        // Make sure that each interval is really equal query.
        singleValueIntervalArray.foreach(interval => {
          assert(interval.start == interval.end)
        })
        if (singleValueIntervalArray.nonEmpty) {
          keySchema = new StructType().add(meta.schema(entries(lastIdx)))
          scanner = BitMapScanner(bestIndexer)
          logDebug("Bitmap index only supports equal query.")
          scanner.intervalArray = singleValueIntervalArray
        }
      case _ =>
    }

    if (scanner != null && keySchema != null) {
      logDebug("Index Scanner Intervals: " + scanner.intervalArray.mkString(", "))
      scanner.withKeySchema(keySchema)

      scanner.internalLimit_=(
        options.getOrElse(OapFileFormat.OAP_INDEX_SCAN_NUM_OPTION_KEY, "0").toInt)
    }
  }

  def unapply(attribute: String): Option[FilterOptimizer] = {
    if (!filterMap.contains(attribute)) {
      val ordinal = meta.schema.fieldIndex(attribute)
      filterMap.put(attribute, new FilterOptimizer(new StructType().add(meta.schema(ordinal))))
    }
    filterMap.get(attribute)
  }

  def unapply(value: Any): Option[Key] =
    Some(InternalRow(CatalystTypeConverters.convertToCatalyst(value)))

  def unapply(values: Array[Any]): Option[Array[Key]] =
    Some(values.map(value => InternalRow(CatalystTypeConverters.convertToCatalyst(value))))
}

private[oap] object DummyIndexContext extends IndexContext(null) {
  override def getScanner: Option[IndexScanner] = None
  override def unapply(attribute: String): Option[FilterOptimizer] = None
  override def unapply(value: Any): Option[Key] = None
}

private[oap] class FilterOptimizer(keySchema: StructType) {
  val order = GenerateOrdering.create(keySchema)

  def isSingleValueInterval(interval: RangeInterval): Boolean =
    interval.start == interval.end && interval.startInclude && interval.endInclude

  // compare two intervals: return true if interval1.start < interval2.start
  // isNullPredicate is assumed to be "smallest"
  def compareRangeInterval(interval1: RangeInterval, interval2: RangeInterval): Boolean = {
    if (interval1.isNullPredicate || interval2.isNullPredicate) {
      if (interval1.isNullPredicate) return true
      else return false
    }
    if ((interval1.start eq IndexScanner.DUMMY_KEY_START) &&
      (interval2.start ne IndexScanner.DUMMY_KEY_START)) {
      return true
    }
    if(interval2.start eq IndexScanner.DUMMY_KEY_START) {
      return false
    }
    order.compare(interval1.start, interval2.start) < 0
  }
  // unite interval extra to interval base
  // return: true if two intervals are unioned together
  //         false if these two intervals cannot be unioned, since they do not overlap
  def intervalUnion(base: RangeInterval, extra: RangeInterval): Boolean = {
    def union: Boolean = {// union two intervals
      if ((extra.end eq IndexScanner.DUMMY_KEY_END) || order.compare(extra.end, base.end)>0) {
        base.end = extra.end
        base.endInclude = extra.endInclude
        return true
      }
      if (order.compare(extra.end, base.end)==0) {
        base.endInclude = base.endInclude || extra.endInclude
      }
      true
    }// end def union

    // isNull U isNull => isNull
    if (base.isNullPredicate && extra.isNullPredicate) return true
    // isNull U otherFilterPredicate => isNull U otherFilterPredicate
    if (base.isNullPredicate ^ extra.isNullPredicate) return false

    if (base.start eq IndexScanner.DUMMY_KEY_START) {
      if (base.end eq IndexScanner.DUMMY_KEY_END) {
        // base is isNotNullPredicate
        // isNotNull U isNull => isNotNull U isNull
        if (extra.isNullPredicate) return false
        else return true // isNotNull U otherFilterPredicate => isNotNull
      }
      if (extra.start ne IndexScanner.DUMMY_KEY_START) {
        val cmp = order.compare(extra.start, base.end)
        if(cmp>0 || (cmp == 0 && !extra.startInclude && !base.endInclude)) {
          return false // cannot union
        }
      }
      // union two intervals
      union
    }
    else {// base.start is not DUMMY
      if (order.compare(extra.start, base.start)==0) {
        base.startInclude = base.startInclude || extra.startInclude
      }
      if (base.end eq IndexScanner.DUMMY_KEY_END) {
        return true
      }
      val cmp = order.compare(extra.start, base.end)
      if(cmp>0 || (cmp==0 && !extra.startInclude && !base.endInclude)) {
        return false // cannot union
      }
      // union two intervals
      union
    }
  }
  // "Or" operation: (union multiple range intervals which may overlap)
  def addBound(intervalArray1: ArrayBuffer[RangeInterval],
               intervalArray2: ArrayBuffer[RangeInterval] ): ArrayBuffer[RangeInterval] = {
    // firstly, put all intervals to intervalArray1
    intervalArray1 ++= intervalArray2
    if (intervalArray1.isEmpty) {
      return intervalArray1
    }

    // sort the array of interval according to the interval's start key
    // After sorted, isNullPredicate(if have) will be put at the front of the array
    val sortedArray = intervalArray1.sortWith(compareRangeInterval)

    val result = ArrayBuffer(sortedArray.head)
    for(i <- 1 until sortedArray.length) {
      val interval = result.last
      // attr >= value, so it is unnecessary to do subsequent union, just return the result
      if (!interval.isNullPredicate &&
        (interval.end eq IndexScanner.DUMMY_KEY_END) && interval.startInclude) {
        return result
      }
      if(!intervalUnion(interval, sortedArray(i))) {
        // these two intervals do not overlap, thus cannot be unioned,
        // just add the second interval to the result list
        result += sortedArray(i)
      }

    }// end for
    result
  }

  // merge two key and their include identifiers
  def intersect(key1: Key, key2: Key, include1: Boolean, include2: Boolean,
                isEndKey: Boolean): (Key, Boolean) = {
    if (key1 == IndexScanner.DUMMY_KEY_START) {
      (key2, include2)
    }
    else {
      if (key2 == IndexScanner.DUMMY_KEY_START) {
        (key1, include1)
      }
      else { // both key1 and key2 are not Dummy
        if (order.compare(key1, key2) == 0) {
          return (key1, include1 && include2)
        }
        if (order.compare(key1, key2) > 0 ^ isEndKey) {
          (key1, include1)
        }
        else {
          (key2, include2)
        }
      }
    }
  }

  // verify non-empty intervals
  def validate(interval: RangeInterval): Boolean = {
    if ((interval.start ne IndexScanner.DUMMY_KEY_START)
      && (interval.end ne IndexScanner.DUMMY_KEY_END)) {
      if (order.compare(interval.start, interval.end)>0) {
        return false
      }
      if (order.compare(interval.start, interval.end) == 0
        && (!interval.startInclude || !interval.endInclude)) {
        return false
      }
    }
    true
  }

  // "And" operation: (intersect multiple range intervals)
  def mergeBound(intervalArray1: ArrayBuffer[RangeInterval],
                 intervalArray2: ArrayBuffer[RangeInterval] ): ArrayBuffer[RangeInterval] = {
    val intervalArray = for {
      interval1 <- intervalArray1
      interval2 <- intervalArray2
      // isNull & otherPredicate => empty
      if !(interval1.isNullPredicate ^ interval2.isNullPredicate)
    } yield {
      // isNull & isNull => isNull
      if (interval1.isNullPredicate && interval2.isNullPredicate) interval1
      else {
        // this condition contains isNotNull & normalInterval => normalInterval
        val interval = RangeInterval(
          IndexScanner.DUMMY_KEY_START,
          IndexScanner.DUMMY_KEY_END,
          includeStart = true,
          includeEnd = true)

        val re1 = intersect(interval1.start, interval2.start,
          interval1.startInclude, interval2.startInclude, isEndKey = false)
        interval.start = re1._1
        interval.startInclude = re1._2

        val re2 = intersect(interval1.end, interval2.end,
          interval1.endInclude, interval2.endInclude, isEndKey = true)
        interval.end = re2._1
        interval.endInclude = re2._2
        interval
      }
    }
    // retain non-empty intervals
    intervalArray.filter(validate)
  }
}
