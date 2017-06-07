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


package org.apache.spark.sql.execution.datasources.spinach.index

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach._
import org.apache.spark.sql.execution.datasources.spinach.filecache._
import org.apache.spark.sql.execution.datasources.spinach.io.IndexFile
import org.apache.spark.sql.types.StructType

// we scan the index from the smallest to the largest,
// this will scan the B+ Tree (index) leaf node.
private[spinach] class BPlusTreeScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {
  override def canBeOptimizedByStatistics: Boolean = true
  override def toString(): String = "BPlusTreeScanner"
  @transient protected var currentKeyArray: Array[CurrentKey] = _

  var currentKeyIdx = 0

  def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name)
    logDebug("Loading Index File: " + path)
    logDebug("\tFile Szie: " + path.getFileSystem(conf).getFileStatus(path).getLen)
    val indexScanner = IndexFiber(IndexFile(path))
    val indexData: IndexFiberCacheData = FiberCacheManager(indexScanner, conf)
    val root = meta.open(indexData, keySchema)

    _init(root)
  }

  def _init(root : IndexNode): IndexScanner = {
    assert(intervalArray ne null, "intervalArray is null!")
    this.ordering = GenerateOrdering.create(keySchema)
    currentKeyArray = new Array[CurrentKey](intervalArray.length)
    currentKeyIdx = 0 // reset to initialized value for this thread
    intervalArray.zipWithIndex.foreach {
      case(interval: RangeInterval, i: Int) =>
        var order: Ordering[Key] = null
        if (interval.start == IndexScanner.DUMMY_KEY_START) {
          // find the first key in the left-most leaf node
          var tmpNode = root
          while (!tmpNode.isLeaf) tmpNode = tmpNode.childAt(0)
          currentKeyArray(i) = new CurrentKey(tmpNode, 0, 0)
        } else {
          // find the first identical key or the first key right greater than the specified one
          if (keySchema.size > interval.start.numFields) { // exists Dummy_Key
            order = GenerateOrdering.create(StructType(keySchema.dropRight(1)))
          } else order = this.ordering
          currentKeyArray(i) = moveTo(root, interval.start, true, order)
          if (keySchema.size > interval.end.numFields) { // exists Dummy_Key
            order = GenerateOrdering.create(StructType(keySchema.dropRight(1)))
            // find the last identical key or the last key less than the specified one on the left
            this.intervalArray(i).end = moveTo(root, interval.end, false, order).currentKey
          }

        }
        // to deal with the LeftOpen condition
        while (!interval.startInclude &&
          currentKeyArray(i).currentKey != IndexScanner.DUMMY_KEY_END &&
          ordering.compare(interval.start, currentKeyArray(i).currentKey) == 0) {
          // find exactly the key, since it's LeftOpen, skip the equivalent key(s)
          currentKeyArray(i).moveNextKey
        }
    }
    this
  }

  // i: the interval index
  def intervalShouldStop(i: Int): Boolean = { // detect if we need to stop scanning
    if (intervalArray(i).end == IndexScanner.DUMMY_KEY_END) { // Left-Only search
      return false
    }
    if (intervalArray(i).endInclude) { // RightClose
      ordering.compare(
        currentKeyArray(i).currentKey, intervalArray(i).end) > 0
    }
    else { // RightOpen
      ordering.compare(
        currentKeyArray(i).currentKey, intervalArray(i).end) >= 0
    }

  }

  /**
   * search the key that equals to candidate in the IndexNode of B+ tree
   * @param node: the node where binary search is executed
   * @param candidate: the candidate key
   * @param findFirst: indicates whether the goal is to find the
   *                  first appeared key that equals to candidate
   * @param order: the ordering that used to compare two keys
   * @return the CurrentKey object that points to the target key
   * findFirst == true -> find the first appeared key that equals to candidate, this is used
   * to determine the start key that begins the scan.
   * In this case, the first identical key(if found) or
   * the first key greater than the specified one on the right(if not found) is returned;
   * findFirst == false -> find the last appeared key that equals to candidate, this is used
   * to determine the end key that terminates the scan.
   * In this case, the last identical key(if found) or
   * the last key less than the specified one on the left(if not found) is returned.
   */
  protected def moveTo(node: IndexNode, candidate: Key, findFirst: Boolean, order: Ordering[Key])
  : CurrentKey = {
    var s = 0
    var e = node.length - 1
    var notFind = true

    var m = s
    while (s <= e & notFind) {
      m = (s + e) / 2
      val cmp = order.compare(node.keyAt(m), candidate)
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
    else { // the candidate key is found in the B+ tree
      if (findFirst) {// if the goal is to find the first appeared key that equals to candidate
        // if the goal is to find the start key,
        // then find the last key that is less than the specified one on the left
        // is always necessary in all Non-Leaf layers(considering the multi-column search)
        while (m>0 && order.compare(node.keyAt(m), candidate) == 0) {m -= 1}
        if (order.compare(node.keyAt(m), candidate) < 0) notFind = true
      } else {
        while (m<node.length-1 && order.compare(node.keyAt(m + 1), candidate) == 0)
          m += 1
      }
    }

    if (node.isLeaf) {
      // here currentKey is equal to candidate or the last key on the left side
      // which is less than the candidate
      val currentKey = new CurrentKey(node, m, 0)

      if (notFind && findFirst) {
        // if not found and the goal is to find the start key, then let's move forward a key
        // if the goal is to find the end key, no need to move next
        if (order.compare(node.keyAt(m), candidate) < 0) {// if current key < candidate
          currentKey.moveNextKey
        }
      }
      currentKey
    } else {
      moveTo(node.childAt(m), candidate, findFirst, order)
    }
  }


  override def hasNext: Boolean = {
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

}
