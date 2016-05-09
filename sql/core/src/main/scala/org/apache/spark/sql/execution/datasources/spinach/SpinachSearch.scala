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

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortDirection, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.tools.nsc.doc.doclet.Indexer

private[spinach] object RangeScanner {
  val DUMMY_KEY_START: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
  val DUMMY_KEY_END: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
}

private[spinach] class CurrentKey(node: IndexNode, keyIdx: Int, valueIdx: Int) {
  assert(node.isLeaf, "Should be Leaf Node")

  private var currentNode: IndexNode = node
  // currentKeyIdx is the flag that we check if we are in the end of the tree traversal
  private var currentKeyIdx: Int = if (node.length > keyIdx) keyIdx else -1
  private var currentValueIdx: Int = valueIdx

  private var currentValues: IndexNodeValue = if (currentKeyIdx != -1) {
    currentNode.valueAt(currentKeyIdx)
  } else {
    null
  }

  def currentKey: Key = if (currentKeyIdx == -1) {
    RangeScanner.DUMMY_KEY_END
  } else {
    currentNode.keyAt(currentKeyIdx)
  }

  def currentRowId: Int = currentValues(currentValueIdx)

  def moveNext: Unit = {
    if (currentValueIdx < currentValues.length ) {
      currentValueIdx += 1
    } else if (currentKeyIdx < currentNode.length) {
      currentKeyIdx += 1
      currentValueIdx = 0
      currentValues = currentNode.valueAt(currentKeyIdx)
    } else {
      currentNode = currentNode.next
      if (currentNode != null) {
        currentKeyIdx = 0
        currentValueIdx = 0
        currentValues = currentNode.valueAt(currentKeyIdx)
      }
    }
  }

  def isEnd: Boolean = (currentNode == null || (currentKey eq RangeScanner.DUMMY_KEY_END))
}

// we scan the index from the smallest to the greatest
private[spinach] trait RangeScanner extends Iterator[Int] {
  protected var currentKey: CurrentKey = _
  protected var ordering: Ordering[Key] = _

  def meta: IndexMeta
  def start: Key

  def initialize(context: TaskAttemptContext): Unit = {
    val root = meta.open(context)
    this.ordering = root.ordering

    if (start eq RangeScanner.DUMMY_KEY_START) {
      // find the first key in the left-most leaf node
      var tmpNode = root
      while (tmpNode.isLeaf == false) tmpNode = tmpNode.childAt(0)
      currentKey = new CurrentKey(tmpNode, 0, 0)
    } else {
      // find the identical key or the first key right greater than the specified one
      moveTo(root, start)
    }
  }

  def shouldStop(key: CurrentKey): Boolean

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
      // if we didn't find the exact value, then we need to pick the right close one
      m = e + 1
    }

    if (node.isLeaf) {
      currentKey = new CurrentKey(node, m, 0)
    } else {
      moveTo(node.childAt(m), candidate)
    }
  }

  override def hasNext: Boolean = {
    currentKey.isEnd
  }

  override def next(): Int = {
    val rowId = currentKey.currentRowId
    currentKey.moveNext
    rowId
  }

  def withNewStart(key: Key, include: Boolean): RangeScanner
  def withNewEnd(key: Key, include: Boolean): RangeScanner
}

private[spinach] object DUMMY_SCANNER extends RangeScanner {
  override def shouldStop(key: CurrentKey): Boolean = true
  override def initialize(context: TaskAttemptContext): Unit = {}
  override def hasNext: Boolean = false
  override def next(): Int = throw new NoSuchElementException("end of iterating.")
  override def withNewStart(key: Key, include: Boolean): RangeScanner = this
  override def withNewEnd(key: Key, include: Boolean): RangeScanner = this
  override def meta: IndexMeta = throw new NotImplementedError()
  override def start: Key = throw new NotImplementedError()
}

private[spinach] trait LeftOpenInitialize extends RangeScanner {
  override def initialize(context: TaskAttemptContext): Unit = {
    super.initialize(context)
    if (ordering.compare(start, currentKey.currentKey) == 0) {
      // find the exactly the key, since it's LeftOpen, skip the first key
      currentKey.moveNext
    }
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

private[spinach] trait IndexNodeValue {
  def length: Int
  def apply(idx: Int): Int
}

private[spinach] trait IndexNode {
  def length: Int
  def keyAt(idx: Int): Key
  def childAt(idx: Int): IndexNode
  def valueAt(idx: Int): IndexNodeValue
  def next: IndexNode
  def isLeaf: Boolean
  def ordering: Ordering[Key]
}

private[spinach] class ScannerBuilder(meta: IndexMeta, ordering: Ordering[Key]) {
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
    scanner
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
    val ordering = GenerateOrdering.create(StructType(fields))

    new ScannerBuilder(meta, ordering)
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

private[spinach] class IndexContext(meta: DataSourceMeta2) {
  // TODO currently only a single attribute index supported.
  private val map = new mutable.HashMap[String, Option[ScannerBuilder]]()
  def unapply(attribute: String): Option[ScannerBuilder] =
    map.getOrElseUpdate(attribute, findIndexer(attribute))

  def unapply(value: Any): Option[Key] = Some(InternalRow(value))

  private def findIndexer(attribute: String): Option[ScannerBuilder] = {
    val ordinal = meta.schema.fieldIndex(attribute)

    var idx = 0
    while (idx < meta.indexMetas.length) {
      meta.indexMetas(idx) match {
        case BTreeIndex(BTreeIndexEntry(ord, dir) :: Nil) if ord == ordinal =>
          assert(dir == Ascending, "we assume the data are sorted in ascending")
          return Some(ScannerBuilder(meta.schema(ordinal), meta.indexMetas(idx), dir))
        case BTreeIndex(entries) => entries.map { entry =>
          throw new UnsupportedOperationException(s"Unsupported multi-keys $entries")
        }
        case other => // we don't support other types of index
          throw new UnsupportedOperationException(s"Unsupported Index Type $other")
      }

      idx += 1
    }

    None
  }
}

private[spinach] object Indexer {
  def apply(attribute: String, meta: DataSourceMeta2, dir: SortDirection): Indexer =
    throw new NotImplementedError()
}

private[spinach] class SpinachSearch(filters: Array[Filter], meta: DataSourceMeta2)
  extends Logging {

  def extract(filter: Filter, ic: IndexContext): Option[Filter] = {
    filter match {
      case EqualTo(ic(indexer), ic(key)) =>
        indexer.withStart(key, true).withEnd(key, false)
        None
      case GreaterThanOrEqual(ic(indexer), ic(key)) =>
        indexer.withStart(key, true)
        None
      case GreaterThan(ic(indexer), ic(key)) =>
        indexer.withStart(key, false)
        None
      case LessThanOrEqual(ic(indexer), ic(key)) =>
        indexer.withEnd(key, true)
        None
      case LessThan(ic(indexer), ic(key)) =>
        indexer.withEnd(key, false)
        None
      case And(left, right) => (extract(left, ic), extract(right, ic)) match {
        case (None, Some(opt)) => Some(opt)
        case (Some(opt), None) => Some(opt)
        case (Some(opt1), Some(opt2)) => Some(And(opt1, opt2))
        case (None, None) => None
      }
      case Or(left, right) => Some(filter) // ignore OR for now TODO
      case _ => Some(filter) // TODO add more filter support
    }
  }

  val unHandledFilter = extract(filters.reduceLeft(And), new IndexContext(meta))
}
