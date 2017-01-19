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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortDirection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.{IndexUtils, SpinachUtils}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.Platform


private[spinach] object RangeScanner {
  val DUMMY_KEY_START: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
  val DUMMY_KEY_END: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
}

private[spinach] object CurrentKey {
  val INVALID_KEY_INDEX = -1
}


// B+ tree values in the leaf node, in long term, a single value should be associated
// with a single key, however, in order to eliminate the duplicated key in the B+ tree,
// we simply take out the values for the identical keys, and keep only a single key in the
// B+ tree leaf node
private[spinach] trait IndexNodeValue {
  def length: Int
  def apply(idx: Int): Long
}

// B+ Tree Node
private[spinach] trait IndexNode {
  def length: Int
  def keyAt(idx: Int): Key
  def childAt(idx: Int): IndexNode
  def valueAt(idx: Int): IndexNodeValue
  def next: IndexNode
  def isLeaf: Boolean
}

trait UnsafeIndexTree {
  def buffer: DataFiberCache
  def offset: Long
  def baseObj: Object = buffer.fiberData.getBaseObject
  def baseOffset: Long = buffer.fiberData.getBaseOffset
  def length: Int = Platform.getInt(baseObj, baseOffset + offset)
}

private[spinach] case class UnsafeIndexNodeValue(
    buffer: DataFiberCache,
    offset: Long,
    dataEnd: Long) extends IndexNodeValue with UnsafeIndexTree {
  // 4 <- value1, 8 <- value2
  override def apply(idx: Int): Long = Platform.getLong(baseObj, baseOffset + offset + 4 + idx * 8)

  // for debug
  private def values: Seq[Long] = (0 until length).map(apply)
  override def toString: String = "ValuesNode(" + values.mkString(",") + ")"
}

private[spinach] case class UnsafeIndexNode(
    buffer: DataFiberCache,
    offset: Long,
    dataEnd: Long,
    schema: StructType) extends IndexNode with UnsafeIndexTree {
  override def keyAt(idx: Int): Key = {
    // 16 <- value5, 12(4 + 8) <- value3 + value4
    val keyOffset = Platform.getLong(baseObj, baseOffset + offset + 12 + idx * 16)
    val len = Platform.getInt(baseObj, baseOffset + keyOffset)
//     val row = new UnsafeRow(schema.length) // this is for debug use
    val row = UnsafeIndexNode.row.get
    row.setNumFields(schema.length)
    row.pointTo(baseObj, baseOffset + keyOffset + 4, len)
    row
  }

  private def treeChildAt(idx: Int): UnsafeIndexTree = {
    // 16 <- value5, 20(4 + 8 + 8) <- value3 + value4 + value5/2
    val childOffset = Platform.getLong(baseObj, baseOffset + offset + 16 * idx + 20)
    if (isLeaf) {
      UnsafeIndexNodeValue(buffer, childOffset, dataEnd)
    } else {
      UnsafeIndexNode(buffer, childOffset, dataEnd, schema)
    }
  }

  override def childAt(idx: Int): UnsafeIndexNode =
    treeChildAt(idx).asInstanceOf[UnsafeIndexNode]
  override def valueAt(idx: Int): UnsafeIndexNodeValue =
    treeChildAt(idx).asInstanceOf[UnsafeIndexNodeValue]
  // if the first child offset is in data segment (treeChildAt(0)), 20 <- 16 * 0 + 20
  override def isLeaf: Boolean = Platform.getLong(baseObj, baseOffset + offset + 20) < dataEnd
  override def next: UnsafeIndexNode = {
    // 4 <- value3
    val nextOffset = Platform.getLong(baseObj, baseOffset + offset + 4)
    if (nextOffset == -1L) {
      null
    } else {
      UnsafeIndexNode(buffer, nextOffset, dataEnd, schema)
    }
  }

  // for debug
  private def children: Seq[UnsafeIndexTree] = (0 until length).map(treeChildAt)
  private def keys: Seq[Key] = (0 until length).map(keyAt)
  override def toString: String =
    s"[Signs(${keys.map(_.getInt(0)).mkString(",")}) " + children.mkString(" ") + "]"
}

private[spinach] object UnsafeIndexNode {
  lazy val row = new ThreadLocal[UnsafeRow] {
    override def initialValue = new UnsafeRow
  }
}

private[spinach] class CurrentKey(node: IndexNode, keyIdx: Int, valueIdx: Int) {
  assert(node.isLeaf, "Should be Leaf Node")

  private var currentNode: IndexNode = node
  // currentKeyIdx is the flag that we check if we are in the end of the tree traversal
  private var currentKeyIdx: Int = if (node.length > keyIdx) {
    keyIdx
  } else {
    CurrentKey.INVALID_KEY_INDEX
  }

  private var currentValueIdx: Int = valueIdx

  private var currentValues: IndexNodeValue = if (currentKeyIdx != CurrentKey.INVALID_KEY_INDEX) {
    currentNode.valueAt(currentKeyIdx)
  } else {
    null
  }

  def currentKey: Key = if (currentKeyIdx == CurrentKey.INVALID_KEY_INDEX) {
    RangeScanner.DUMMY_KEY_END
  } else {
    currentNode.keyAt(currentKeyIdx)
  }

  def currentRowId: Long = currentValues(currentValueIdx)

  def moveNextValue: Unit = {
    if (currentValueIdx < currentValues.length - 1) {
      currentValueIdx += 1
    } else {
      moveNextKey
    }
  }

  def moveNextKey: Unit = {
    if (currentKeyIdx < currentNode.length - 1) {
      currentKeyIdx += 1
      currentValueIdx = 0
      currentValues = currentNode.valueAt(currentKeyIdx)
    } else {
      currentNode = currentNode.next
      if (currentNode != null) {
        currentKeyIdx = 0
        currentValueIdx = 0
        currentValues = currentNode.valueAt(currentKeyIdx)
      } else {
        currentKeyIdx = CurrentKey.INVALID_KEY_INDEX
      }
    }
  }

  def isEnd: Boolean = currentNode == null || (currentKey == RangeScanner.DUMMY_KEY_END)
}

// we scan the index from the smallest to the greatest, this is the root class
// of scanner, which will scan the B+ Tree (index) leaf node.
private[spinach] class RangeScanner(idxMeta: IndexMeta) extends Iterator[Long] with Serializable {
  // TODO this is a temp work around
  override def toString(): String = "RangeScanner"
//  @transient protected var currentKey: CurrentKey = _
  @transient protected var currentKeyArray: Array[CurrentKey] = _
  @transient protected var ordering: Ordering[Key] = _
  var intervalArray: ArrayBuffer[RangeInterval] = _
  protected var keySchema: StructType = _

  def meta: IndexMeta = idxMeta
//  def start: Key = null // the start node

//  val startArray = new ArrayBuffer[Key]()
//  val endArray = new ArrayBuffer[Key]()
//  val stInclude = new ArrayBuffer[Boolean]()
//  val endInclude = new ArrayBuffer[Boolean]()
  var currentKeyIdx = 0

  def exist(dataPath: Path, conf: Configuration): Boolean = {
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name)
    path.getFileSystem(conf).exists(path)
  }


  def initialize(dataPath: Path, conf: Configuration): RangeScanner = {
    assert(keySchema ne null)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name)
    val indexScanner = IndexFiber(IndexFile(path))
    val indexData: IndexFiberCacheData = FiberCacheManager(indexScanner, conf)
    val root = meta.open(indexData, keySchema)

    _init(root)
  }

  def _init(root : IndexNode): RangeScanner = {
    assert(intervalArray ne null, "intervalArray is null!")
    this.ordering = GenerateOrdering.create(keySchema)
    currentKeyArray = new Array[CurrentKey](intervalArray.length)
    currentKeyIdx = 0 // reset to initialized value for this thread
    intervalArray.zipWithIndex.foreach {
      case(interval: RangeInterval, i: Int) =>
      if (interval.start == RangeScanner.DUMMY_KEY_START) {
        // find the first key in the left-most leaf node
        var tmpNode = root
        while (!tmpNode.isLeaf) tmpNode = tmpNode.childAt(0)
        currentKeyArray(i) = new CurrentKey(tmpNode, 0, 0)
      } else {
        // find the identical key or the first key right greater than the specified one
        moveTo(root, interval.start, i)
      }
      // process the LeftOpen condition
      if (!interval.startInclude
        && currentKeyArray(i).currentKey != RangeScanner.DUMMY_KEY_END) {
        if (ordering.compare(interval.start, currentKeyArray(i).currentKey) == 0) {
          // find the exactly the key, since it's LeftOpen, skip the first key
          currentKeyArray(i).moveNextKey
        }
      }
    }

//    // filter the useless conditions(useless search ranges)
//    currentKeyArray = currentKeyArray.filter(
//      key => !(key.isEnd || shouldStop(key)))

    this
  }

  // i: the interval index
  def intervalShouldStop(i: Int): Boolean = { // detect if we need to stop scanning
    if (intervalArray(i).end == RangeScanner.DUMMY_KEY_END) { // Left-Only search
      return false
    }
    if (intervalArray(i).endInclude) { // RightClose
      ordering.compare(
        currentKeyArray(i).currentKey, intervalArray(i).end) > 0
    }
    else { // RightOpen
//      val k = currentKeyArray(i).currentKey
      ordering.compare(
        currentKeyArray(i).currentKey, intervalArray(i).end) >= 0
    }

  }

  protected def moveTo(node: IndexNode, candidate: Key, keyIdx: Int): Unit = {
    var s = 0
    var e = node.length - 1
    var notFind = true

    var m = s
    while (s <= e & notFind) {
      m = (s + e) / 2
      val cmp = ordering.compare(node.keyAt(m), candidate)
      if (cmp == 0) {
        notFind = false
      } else if (cmp < 0) {
        s = m + 1
      } else {
        e = m - 1
      }
    }

    if (notFind) {
      m = if (e < 0) 0 else e
    }

    if (node.isLeaf) {
      // here currentKey is equal to candidate or the last key in the left
      // which is less than the candidate
      currentKeyArray(keyIdx) = new CurrentKey(node, m, 0)

      if (notFind) {
        // if not find, then let's move forward a key
        if (ordering.compare(node.keyAt(m), candidate) < 0) {// if current key < candidate
          currentKeyArray(keyIdx).moveNextKey
        }

      }
    } else {
      moveTo(node.childAt(m), candidate, keyIdx)
    }
  }

//  override def hasNext: Boolean = !(currentKey.isEnd || shouldStop(currentKey))
override def hasNext: Boolean = {
//  intervalArray.nonEmpty && !(currentKeyIdx == currentKeyArray.length-1 &&
//    (currentKeyArray(currentKeyIdx).isEnd || intervalShouldStop(currentKeyIdx)) )
  if (intervalArray.isEmpty) return false
  for(i <- currentKeyIdx until currentKeyArray.length) {
    if (!currentKeyArray(i).isEnd && !intervalShouldStop(i)) {
      return true
    }
  }// end for
  false
}

  override def next(): Long = {
    while (currentKeyArray(currentKeyIdx).isEnd || intervalShouldStop(currentKeyIdx)) {
      currentKeyIdx += 1
    }
    val rowId = currentKeyArray(currentKeyIdx).currentRowId
    currentKeyArray(currentKeyIdx).moveNextValue
    rowId
  }

  def withKeySchema(schema: StructType): RangeScanner = {
    this.keySchema = schema
    this
  }
//  def withNewStart(key: Key, include: Boolean): RangeScanner
//  def withNewEnd(key: Key, include: Boolean): RangeScanner
}

private[spinach] case class BloomFilterScanner(me: IndexMeta) extends RangeScanner(me) {
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

  override def initialize(inputPath: Path, configuration: Configuration): RangeScanner = {
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
    stopFlag = if (equalValues != null && equalValues.length > 0) {
      !equalValues.map(value => bloomFilter
        .checkExist(value.getInt(0).toString)) // TODO getValue needs to be optimized
        .reduceOption(_ || _).getOrElse(false)
    } else false
    curIdx = 0
    this
  }

  override def toString: String = "BloomFilterScanner"
}

// A dummy scanner will actually not do any scanning
private[spinach] object DUMMY_SCANNER extends RangeScanner(null) {
  //  override def shouldStop(key: CurrentKey): Boolean = true
  override def intervalShouldStop(i: Int): Boolean = true
  override def initialize(path: Path, configuration: Configuration): RangeScanner = { this }
  override def hasNext: Boolean = false
  override def next(): Long = throw new NoSuchElementException("end of iterating.")
//  override def withNewStart(key: Key, include: Boolean): RangeScanner = this
//  override def withNewEnd(key: Key, include: Boolean): RangeScanner = this
  override def meta: IndexMeta = throw new NotImplementedError()
//  override def start: Key = throw new NotImplementedError()
}

private[spinach] class ScannerBuilder(meta: IndexMeta, keySchema: StructType) {
  private var scanner: RangeScanner = _

  private val order = GenerateOrdering.create(keySchema)

  // compare two intervals
  def compare(interval1: RangeInterval, interval2: RangeInterval): Boolean = {
    if ((interval1.start eq RangeScanner.DUMMY_KEY_START) &&
      (interval2.start ne RangeScanner.DUMMY_KEY_START)) {
      return true
    }
    if(interval2.start eq RangeScanner.DUMMY_KEY_START) {
      return false
    }
    order.compare(interval1.start, interval2.start) < 0
  }
  // unite interval extra to interval base
  // return: if two intervals is unioned
  def intervalUnion(base: RangeInterval, extra: RangeInterval): Boolean = {
    def union: Boolean = {// union two intervals
      if ((extra.end eq RangeScanner.DUMMY_KEY_END) || order.compare(extra.end, base.end)>0) {
        base.end = extra.end
        base.endInclude = extra.endInclude
        return true
      }
      if (order.compare(extra.end, base.end)==0) {
        base.endInclude = base.endInclude || extra.endInclude
      }
      true
    }// end def union

    if (base.start eq RangeScanner.DUMMY_KEY_START) {
      if (base.end eq RangeScanner.DUMMY_KEY_END) {
        return true
      }
      if (extra.start ne RangeScanner.DUMMY_KEY_START) {
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
      if (base.end eq RangeScanner.DUMMY_KEY_END) {
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
  // Or operation: (union multiple range intervals which may overlap)
  def addBound(intervalArray1: ArrayBuffer[RangeInterval],
               intervalArray2: ArrayBuffer[RangeInterval] ): ArrayBuffer[RangeInterval] = {
    // firstly, put all intervals to intervalArray1
    intervalArray1 ++= intervalArray2
    if (intervalArray1.isEmpty) {
      return intervalArray1
    }

    intervalArray1.sortWith(compare)

    val result = ArrayBuffer(intervalArray1.head)
    for(i <- 1 until intervalArray1.length) {
      val interval = result.last
      if ((interval.end eq RangeScanner.DUMMY_KEY_END) && interval.startInclude) {
        return result
      }
      if(!intervalUnion(interval, intervalArray1(i))) {
        result += intervalArray1(i)
      }

    }// end for
    result
  }

  // merge two key and their include identifiers
  def intersect(key1: Key, key2: Key, include1: Boolean, include2: Boolean,
                isEndKey: Boolean): (Key, Boolean) = {
    if (key1 == RangeScanner.DUMMY_KEY_START) {
      (key2, include2)
    }
    else {
      if (key2 == RangeScanner.DUMMY_KEY_START) {
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
    if ((interval.start ne RangeScanner.DUMMY_KEY_START)
      && (interval.end ne RangeScanner.DUMMY_KEY_END)) {
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

  // And operation: (intersect multiple range intervals)
  def mergeBound(intervalArray1: ArrayBuffer[RangeInterval],
                 intervalArray2: ArrayBuffer[RangeInterval] ): ArrayBuffer[RangeInterval] = {
    val intervalArray = for {
      interval1 <- intervalArray1
      interval2 <- intervalArray2
    } yield {
      val interval = new RangeInterval(
        RangeScanner.DUMMY_KEY_START, RangeScanner.DUMMY_KEY_END, true, true)

      val re1 = intersect(interval1.start, interval2.start,
          interval1.startInclude, interval2.startInclude, false)
      interval.start = re1._1
      interval.startInclude = re1._2

      val re2 = intersect(interval1.end, interval2.end,
          interval1.endInclude, interval2.endInclude, true)
      interval.end = re2._1
      interval.endInclude = re2._2
      interval
    }
    // retain non-empty intervals
    intervalArray.filter(validate)
  }

//  def withStart(s: Key, include: Boolean): ScannerBuilder = {
//    if (scanner == null) {
//      if (include) {
//        scanner = LeftCloseRangeSearch(meta, s)
//      } else {
//        scanner = LeftOpenRangeSearch(meta, s)
//      }
//    } else {
//      scanner = scanner.withNewStart(s, include)
//    }
//
//    startArrayBuffer(startArrayBuffer.length - 1) =
//      if (startArrayBuffer.last == RangeScanner.DUMMY_KEY_START
//        || order.compare(s, startArrayBuffer.last) > 0) {s} else startArrayBuffer.last
//
//    this
//  }
//
//  def withEnd(e: Key, include: Boolean): ScannerBuilder = {
// //    if (scanner == null) {
// //      if (include) {
// //        scanner = RightCloseRangeSearch(meta, e)
// //      } else {
// //        scanner = RightOpenRangeSearch(meta, e)
// //      }
// //    } else {
// //      scanner = scanner.withNewEnd(e, include)
// //    }
//
//    endArrayBuffer(endArrayBuffer.length - 1) =
//      if (endArrayBuffer.last == RangeScanner.DUMMY_KEY_END
//        || order.compare(e, endArrayBuffer.last) < 0) {e} else endArrayBuffer.last
//
//    this
//  }

  def buildScanner(intervalArray: ArrayBuffer[RangeInterval]): Unit = {
    intervalArray.sortWith(compare)
    scanner = meta.indexType match {
      case BloomFilterIndex(entries) =>
        BloomFilterScanner(meta)
      case _ =>
        new RangeScanner(meta)
    }
    scanner.intervalArray = intervalArray
  }

  def build: RangeScanner = {
    assert(scanner ne null, "Scanner is not set")
    scanner.withKeySchema(keySchema)
  }
}

private[spinach] object ScannerBuilder {
  /**
   * Build the scanner builder with multiple keys
   *
   * @param fields
   * @param meta
   * @param dirs
   * @return
   */
  def apply(fields: Seq[StructField], meta: IndexMeta, dirs: Seq[SortDirection])
  : ScannerBuilder = {
    // TODO default we use the Ascending order
    // val ordering = GenerateOrdering.create(StructType(fields))
    val keySchema = StructType(fields)
    new ScannerBuilder(meta, keySchema)
  }

  /**
   * For scanner with no direction
   * @param field to build a schema
   * @param meta meta info
   * @return
   */
  def apply(field: StructField, meta: IndexMeta): ScannerBuilder = {
    val keySchema = new StructType().add(field)
    new ScannerBuilder(meta, keySchema)
  }

  /**
   * Build the scanner builder while indexed field contains only a single key
   *
   * @param field the indexed field with name & data type
   * @param meta the index meta info
   * @param dir the direction of the index data (Ascending or Descending)
   * @return the Scanner Builder
   */
  def apply(field: StructField, meta: IndexMeta, dir: SortDirection): ScannerBuilder = {
    apply(new StructType().add(field), meta, dir :: Nil)
  }
}

// TODO currently only a single attribute index supported.
private[spinach] class IndexContext(meta: DataSourceMeta) {
  private val map = new scala.collection.mutable.HashMap[String, ScannerBuilder]()

  def clear(): IndexContext = {
    map.clear()
    this
  }

  def getScannerBuilder: Option[ScannerBuilder] = {
    if (map.size == 0) {
      None
    } else if (map.size == 1) {
      Some(map.iterator.next()._2)
    } else {
      throw new UnsupportedOperationException("currently only a single index supported")
    }
  }

  def unapply(attribute: String): Option[ScannerBuilder] = {
    if (!map.contains(attribute)) {
      findIndexer(attribute) match {
        case Some(scannerBuilder) => map.update(attribute, scannerBuilder)
        case None =>
      }
    }
    map.get(attribute)
  }

  def unapply(value: Any): Option[Key] =
    Some(InternalRow(CatalystTypeConverters.convertToCatalyst(value)))

  private def findIndexer(attribute: String): Option[ScannerBuilder] = {
    val ordinal = meta.schema.fieldIndex(attribute)

    var idx = 0
    while (idx < meta.indexMetas.length) {
      meta.indexMetas(idx).indexType match {
        case BTreeIndex(entries) if (entries.length == 1 && entries(0).ordinal == ordinal) =>
          // assert(dir == Ascending, "we assume the data are sorted in ascending")
          // TODO currently we are only support the Ascending
          return Some(ScannerBuilder(meta.schema(ordinal), meta.indexMetas(idx), Ascending))
        case BTreeIndex(entries) => entries.map { entry =>
          // TODO support multiple key in the index
        }
        case BloomFilterIndex(entries) if entries.indexOf(ordinal) >= 0 =>
          // TODO support muliple key in the index
          return Some(ScannerBuilder(meta.schema(ordinal), meta.indexMetas(idx)))
        case other => // we don't support other types of index
        // TODO support the other types of index
      }

      idx += 1
    }

    None
  }
}

private[spinach] object DummyIndexContext extends IndexContext(null) {
  override def getScannerBuilder: Option[ScannerBuilder] = None
  override def unapply(attribute: String): Option[ScannerBuilder] = None
  override def unapply(value: Any): Option[Key] = None
}

private [spinach] class RangeInterval(s: Key, e: Key, includeStart: Boolean, includeEnd: Boolean)
  extends Serializable{
  var start = s
  var end = e
  var startInclude = includeStart
  var endInclude = includeEnd
}
private [spinach] object RangeInterval{
  def apply(s: Key, e: Key, includeStart: Boolean, includeEnd: Boolean): RangeInterval
  = new RangeInterval(s, e, includeStart, includeEnd)
}

// The build the BPlushTree Search Scanner according to the filter and indices,
private[spinach] object BPlusTreeSearch extends Logging {
  def optimizeFilterBound(filter: Filter, ic: IndexContext)
  : mutable.HashMap[String, ArrayBuffer[RangeInterval]] = {
    filter match {
      case And(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        for((attribute, intervals) <- rightMap) {
          if (leftMap.contains(attribute)) {
            attribute match {
            case ic (sBuilder) => // extract the corresponding scannerBuilder
              // combine all intervals of the same attribute of leftMap and rightMap
            leftMap.put (
            attribute, sBuilder.mergeBound (leftMap.getOrElseUpdate (attribute, null), intervals) )
            case _ => // this attribute is not index, do nothing
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
            case ic (sBuilder) => // extract the corresponding scannerBuilder
              // add bound of the same attribute to the left map
              leftMap.put (
                attribute, sBuilder.addBound (leftMap.getOrElse (attribute, null), intervals) )
            case _ => // this attribute is not index, do nothing
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
      case _ => null// new mutable.HashMap[String, ArrayBuffer[RangeInterval]]()
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
  // TODO support multiple scanner & And / Or
  def build(filters: Array[Filter], ic: IndexContext): Array[Filter] = {
//    def buildScannerBound2(filter: Filter, k: Int): Boolean = {
//      filter match {
//        case EqualTo(attribute, ic(key)) =>
//          val ic(sBuilder) = attribute
//          sBuilder.withStart(key, true).withEnd(key, true)
//          false
//        case GreaterThanOrEqual(ic(indexer), ic(key)) =>
//          indexer.withStart(key, true)
//          false
//        case GreaterThan(ic(indexer), ic(key)) =>
//          indexer.withStart(key, false)
//          false
//        case LessThanOrEqual(ic(indexer), ic(key)) =>
//          indexer.withEnd(key, true)
//          false
//        case LessThan(ic(indexer), ic(key)) =>
//          indexer.withEnd(key, false)
//          false
//        case _ => true
//      }
//    }

    if (filters == null || filters.isEmpty) return filters
    val intervalMapArray = filters.map(optimizeFilterBound(_, ic))
    // reduce multiple hashMap to one hashMap
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
                case ic (sBuilder) => // extract the corresponding scannerBuilder
                // combine all intervals of the same attribute of leftMap and rightMap
                  leftMap.put (attribute,
                sBuilder.mergeBound (leftMap.getOrElseUpdate (attribute, null), intervals) )
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
    for((attribute, intervalArray) <- intervalMap) {
      attribute match {
        case ic(scannerBuilder) =>
          scannerBuilder.buildScanner(intervalArray)
        case _ => // this attribute is not index, do nothing
      }
    }
//    val retFilters = filters.filter(f => buildScannerBound2(f, 1))
// //  ic.getScannerBuilder.foreach(_.updateBound)
//    retFilters
    filters.filterNot(canSupport(_, ic))
  }

}
