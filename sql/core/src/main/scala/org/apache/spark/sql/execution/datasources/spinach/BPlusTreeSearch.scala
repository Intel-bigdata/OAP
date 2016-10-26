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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortDirection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
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
// with a single key, however, in order to eleminate the duplicated key in the B+ tree,
// we simply take out the values for the identical keys, and keep only a single key in the
// B+ tree leaf node
private[spinach] trait IndexNodeValue {
  def length: Int
  def apply(idx: Int): Int
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

private[spinach] case class InMemoryIndexNodeValue(values: Seq[Int]) extends IndexNodeValue {
  override def length: Int = values.length
  override def apply(idx: Int): Int = values(idx)
  override def toString: String = "ValuesNode(" + values.mkString(",") + ")"
}

private[spinach] case class InMemoryIndexNode(
    keys: Seq[InternalRow],
    children: Seq[IndexNode],
    values: Seq[IndexNodeValue],
    next: IndexNode,
    isLeaf: Boolean) extends IndexNode {
  override def length: Int = keys.length
  override def keyAt(idx: Int): Key = keys(idx)
  override def childAt(idx: Int): IndexNode =
    if (isLeaf) sys.error("No child for index leaf!") else children(idx)
  override def valueAt(idx: Int): IndexNodeValue =
    if (isLeaf) values(idx) else sys.error("No value for index non-leaf!")
  override def toString: String =
    if (isLeaf) {
      s"[Signs(${keys.map(_.getInt(0)).mkString(",")}) " + values.mkString(" ") + "]"
    } else {
      s"[Signs(${keys.map(_.getInt(0)).mkString(",")}) " + children.mkString(" ") + "]"
    }
}

trait UnsafeIndexTree {
  def buffer: FiberCacheData
  def offset: Int
  def baseObj: Object = buffer.fiberData.getBaseObject
  def baseOffset: Long = buffer.fiberData.getBaseOffset
  def length: Int = Platform.getInt(baseObj, baseOffset + offset)
}

private[spinach] case class UnsafeIndexNodeValue(
    buffer: FiberCacheData,
    offset: Int,
    dataEnd: Int) extends IndexNodeValue with UnsafeIndexTree {
  override def apply(idx: Int): Int = Platform.getInt(baseObj, baseOffset + offset + 4 + idx * 4)

  // for debug
  private def values: Seq[Int] = (0 until length).map(apply)
  override def toString: String = "ValuesNode(" + values.mkString(",") + ")"
}

private[spinach] case class UnsafeIndexNode(
    buffer: FiberCacheData,
    offset: Int,
    dataEnd: Int,
    schema: StructType) extends IndexNode with UnsafeIndexTree {
  override def keyAt(idx: Int): Key = {
    val keyOffset = Platform.getInt(baseObj, baseOffset + offset + 8 + idx * 8)
    val len = Platform.getInt(baseObj, baseOffset + keyOffset)
    val row = new UnsafeRow
    row.pointTo(baseObj, baseOffset + keyOffset + 4 + schema.length, len)
    row
  }

  private def treeChildAt(idx: Int): UnsafeIndexTree = {
    val childOffset = Platform.getInt(baseObj, baseOffset + offset + 8 * idx + 12)
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
  override def isLeaf: Boolean = Platform.getInt(baseObj, baseOffset + offset + 12) < dataEnd
  override def next: UnsafeIndexNode = {
    val nextOffset = Platform.getInt(baseObj, baseOffset + offset + 4)
    if (nextOffset == -1) {
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
  // TODO use this to replace UnsafeIndexNode's UnsafeRow creation
  val row = new UnsafeRow
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

  def currentRowId: Int = currentValues(currentValueIdx)

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

  def isEnd: Boolean = (currentNode == null || (currentKey eq RangeScanner.DUMMY_KEY_END))
}

// we scan the index from the smallest to the greatest, this is the root class
// of scanner, which will scan the B+ Tree (index) leaf node.
private[spinach] trait RangeScanner extends Iterator[Int] {
  // TODO this is a temp work around
  override def toString(): String = "RangeScanner"
  @transient protected var currentKey: CurrentKey = _
  @transient protected var ordering: Ordering[Key] = _
  protected var keySchema: StructType = _

  def meta: IndexMeta
  def start: Key // the start node

  def initialize(dataPath: String, conf: Configuration): RangeScanner = {
    assert(keySchema ne null)
    this.ordering = GenerateOrdering.create(keySchema)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileNameFromDataFileName(dataPath, meta.name)
    val indexFiber = IndexFiber(IndexFileScanner(path, keySchema, conf))
    val indexData = IndexCacheManager(indexFiber)
    val root = meta.open(indexData, keySchema)

    if (start eq RangeScanner.DUMMY_KEY_START) {
      // find the first key in the left-most leaf node
      var tmpNode = root
      while (tmpNode.isLeaf == false) tmpNode = tmpNode.childAt(0)
      currentKey = new CurrentKey(tmpNode, 0, 0)
    } else {
      // find the identical key or the first key right greater than the specified one
      moveTo(root, start)
    }
    this
  }

  def shouldStop(key: CurrentKey): Boolean // detect if we need to stop scanning

  protected def moveTo(node: IndexNode, candidate: Key): Unit = {
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
      currentKey = new CurrentKey(node, m, 0)
      if (notFind) {
        // if not find, then let's move forward a key
        currentKey.moveNextValue
      }
    } else {
      moveTo(node.childAt(m), candidate)
    }
  }

  override def hasNext: Boolean = !(currentKey.isEnd || shouldStop(currentKey))

  override def next(): Int = {
    val rowId = currentKey.currentRowId
    currentKey.moveNextValue
    rowId
  }

  def withKeySchema(schema: StructType): RangeScanner = {
    this.keySchema = schema
    this
  }
  def withNewStart(key: Key, include: Boolean): RangeScanner
  def withNewEnd(key: Key, include: Boolean): RangeScanner
}

// A dummy scanner will actually not do any scanning
private[spinach] object DUMMY_SCANNER extends RangeScanner {
  override def shouldStop(key: CurrentKey): Boolean = true
  override def initialize(path: String, configuration: Configuration): RangeScanner = { this }
  override def hasNext: Boolean = false
  override def next(): Int = throw new NoSuchElementException("end of iterating.")
  override def withNewStart(key: Key, include: Boolean): RangeScanner = this
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = this
  override def meta: IndexMeta = throw new NotImplementedError()
  override def start: Key = throw new NotImplementedError()
}

private[spinach] trait LeftOpenInitialize extends RangeScanner {
  override def initialize(path: String, conf: Configuration): RangeScanner = {
    super.initialize(path, conf)
    if (ordering.compare(start, currentKey.currentKey) == 0) {
      // find the exactly the key, since it's LeftOpen, skip the first key
      currentKey.moveNextKey
    }
    this
  }
}

private[spinach] trait LeftShouldStop {
  def shouldStop(key: CurrentKey): Boolean = false
}

private[spinach] trait RightInitialize extends RangeScanner {
  def start: Key = RangeScanner.DUMMY_KEY_START
}

private[spinach] trait RightOpenShouldStop extends RangeScanner {
  def end: Key
  def shouldStop(key: CurrentKey): Boolean = ordering.compare(key.currentKey, end) >= 0
}

private[spinach] trait RightCloseShouldStop extends RangeScanner {
  def end: Key
  def shouldStop(key: CurrentKey): Boolean = ordering.compare(key.currentKey, end) > 0
}

// scan range (start, -), start key will be ignored
private[spinach] case class LeftOpenRangeSearch(meta: IndexMeta, start: Key)
  extends LeftOpenInitialize with LeftShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, start) >= 0) {
      if (include) {
        LeftCloseRangeSearch(meta, key)
      } else {
        LeftOpenRangeSearch(meta, key)
      }
    } else {
      this
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (include) {
      LeftOpenRightCloseRangeSearch(meta, start, key)
    } else {
      LeftOpenRightOpenRangeSearch(meta, start, key)
    }
  }
}

// scan range [start, -), start key will be included
private[spinach] case class LeftCloseRangeSearch(meta: IndexMeta, start: Key)
  extends RangeScanner with LeftShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, start) > 0) {
      if (include) LeftCloseRangeSearch(meta, key) else LeftOpenRangeSearch(meta, key)
    } else {
      this
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (include) {
      LeftCloseRightCloseRangeSearch(meta, start, key)
    } else {
      LeftCloseRightOpenRangeSearch(meta, start, key)
    }
  }
}

// scan range (-, end), end key will be ignored
private[spinach] case class RightOpenRangeSearch(meta: IndexMeta, end: Key)
  extends RightInitialize with RightOpenShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (include) {
      LeftCloseRightOpenRangeSearch(meta, key, end)
    } else {
      LeftOpenRightOpenRangeSearch(meta, key, end)
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, end) <= 0) {
      if (include) {
        RightCloseRangeSearch(meta, key)
      } else {
        RightOpenRangeSearch(meta, key)
      }
    } else {
      this
    }
  }
}

// scan range (-, end], end key will be included
private[spinach] case class RightCloseRangeSearch(meta: IndexMeta, end: Key)
  extends RightInitialize with RightCloseShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (include) {
      LeftCloseRightCloseRangeSearch(meta, key, end)
    } else {
      LeftOpenRightOpenRangeSearch(meta, key, end)
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, end) < 0) {
      if (include) {
        RightCloseRangeSearch(meta, key)
      } else {
        RightOpenRangeSearch(meta, key)
      }
    } else {
      this
    }
  }
}

// scan range (start, end), both start & end key will be ignored
private[spinach] case class LeftOpenRightOpenRangeSearch(meta: IndexMeta, start: Key, end: Key)
  extends LeftOpenInitialize with RightOpenShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, start) >= 0) {
      if (include) {
        LeftCloseRightOpenRangeSearch(meta, key, end)
      } else {
        LeftOpenRightOpenRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, end) <= 0) {
      if (include) {
        LeftOpenRightCloseRangeSearch(meta, key, end)
      } else {
        LeftOpenRightOpenRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
}

// scan range [start, end), start key will be included, but end key will be ignored
private[spinach] case class LeftCloseRightOpenRangeSearch(meta: IndexMeta, start: Key, end: Key)
  extends RangeScanner with RightOpenShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, start) > 0) {
      if (include) {
        LeftCloseRightOpenRangeSearch(meta, key, end)
      } else {
        LeftOpenRightOpenRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, end) <= 0) {
      if (include) {
        LeftCloseRightCloseRangeSearch(meta, key, end)
      } else {
        LeftCloseRightOpenRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
}

// scan range (start, end], start key will be ignored, but end key will be included
private[spinach] case class LeftOpenRightCloseRangeSearch(meta: IndexMeta, start: Key, end: Key)
  extends LeftOpenInitialize with RightCloseShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, start) >= 0) {
      if (include) {
        LeftCloseRightCloseRangeSearch(meta, key, end)
      } else {
        LeftOpenRightCloseRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, end) < 0) {
      if (include) {
        LeftOpenRightCloseRangeSearch(meta, key, end)
      } else {
        LeftOpenRightOpenRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
}

// scan range [start, end], both start & end key will be included
private[spinach] case class LeftCloseRightCloseRangeSearch(meta: IndexMeta, start: Key, end: Key)
  extends RangeScanner with RightCloseShouldStop {
  override def withNewStart(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, start) > 0) {
      if (include) {
        LeftCloseRightCloseRangeSearch(meta, key, end)
      } else {
        LeftOpenRightCloseRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = {
    if (ordering.compare(key, end) < 0) {
      if (include) {
        LeftCloseRightCloseRangeSearch(meta, key, end)
      } else {
        LeftCloseRightOpenRangeSearch(meta, key, end)
      }
    } else {
      this
    }
  }
}

private[spinach] class ScannerBuilder(meta: IndexMeta, keySchema: StructType) {
  private var scanner: RangeScanner = _

  def withStart(s: Key, include: Boolean): ScannerBuilder = {
    if (scanner == null) {
      if (include) {
        scanner = LeftCloseRangeSearch(meta, s)
      } else {
        scanner = LeftOpenRangeSearch(meta, s)
      }
    } else {
      scanner = scanner.withNewStart(s, include)
    }

    this
  }

  def withEnd(e: Key, include: Boolean): ScannerBuilder = {
    if (scanner == null) {
      if (include) {
        scanner = RightCloseRangeSearch(meta, e)
      } else {
        scanner = RightOpenRangeSearch(meta, e)
      }
    } else {
      scanner = scanner.withNewEnd(e, include)
    }

    this
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
        case Some(scanner) => map.update(attribute, scanner)
        case None =>
      }
    }
    map.get(attribute)
  }

  def unapply(value: Any): Option[Key] = Some(InternalRow(value))

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

// The build the BPlushTree Search Scanner according to the filter and indices,
private[spinach] object BPlusTreeSearch extends Logging {
  // TODO support multiple scanner & And / Or
  def build(filters: Array[Filter], ic: IndexContext): Array[Filter] = {
    filters.filter(_ match {
      case EqualTo(ic(indexer), ic(key)) =>
        indexer.withStart(key, true).withEnd(key, true)
        false
      case GreaterThanOrEqual(ic(indexer), ic(key)) =>
        indexer.withStart(key, true)
        false
      case GreaterThan(ic(indexer), ic(key)) =>
        indexer.withStart(key, false)
        false
      case LessThanOrEqual(ic(indexer), ic(key)) =>
        indexer.withEnd(key, true)
        false
      case LessThan(ic(indexer), ic(key)) =>
        indexer.withEnd(key, false)
        false
      case _ => true
    })
  }
}
