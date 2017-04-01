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

package org.apache.spark.sql.execution.datasources.spinach.utils

import org.apache.spark.internal.Logging


/**
 * Util functions for building BTree from sorted iterator
 */
object BTreeUtils extends Logging {
  private def branch(n: Long): Unit = if (n > 1000000) {
    // TODO better ways
    logWarning("too big partition for spinach!")
    BRANCHING = math.ceil(math.pow(n, 1.0/3)).toInt
  } else if (n > 125) {
    BRANCHING = math.ceil(math.pow(n, 1.0/3)).toInt
  } else {
    BRANCHING = 5
  }
  private var BRANCHING: Int = 5

  /**
   * Splits an array of n
   */
  private def shape(n: Long, height: Int): Seq[Int] = {
    if (height == 1) {
      return Seq(n.toInt)
    }
    val maxSubTree = math.pow(BRANCHING, height - 1).toLong
    val numOfSubTree = (if (n % maxSubTree == 0) n / maxSubTree else n / maxSubTree + 1).toInt
    val baseSubTreeSize = n / numOfSubTree
    val remainingSize = (n % numOfSubTree).toInt
    (1 to numOfSubTree).map(i => baseSubTreeSize + (if (i <= remainingSize) 1 else 0)).flatMap(
      subSize => shape(subSize, height - 1)
    )
  }

  /**
   * Splits an array of n
   */
  private def shape2(n: Long, height: Int): BTreeNode = {
    if (height == 1) {
      return BTreeNode(n.toInt, Nil)
    }
    val maxSubTree = math.pow(BRANCHING, height - 1).toLong
    val numOfSubTree = (if (n % maxSubTree == 0) n / maxSubTree else n / maxSubTree + 1).toInt
    val baseSubTreeSize = n / numOfSubTree
    val remainingSize = (n % numOfSubTree).toInt
    val children = (1 to numOfSubTree).map(i =>
      baseSubTreeSize + (if (i <= remainingSize) 1 else 0)).map(
        subSize => shape2(subSize, height - 1)
    )
    BTreeNode(numOfSubTree, children)
  }

  /**
   * Estimates height of BTree. `public` for unit test
   */
  def height(size: Long): Int = {
    branch(size)
    val est = if (size > 1) math.ceil(math.log(size) / math.log(BRANCHING)).toInt else 1
    // here we check it reversely to avoid precision problem
    if (math.pow(BRANCHING, est - 1) >= size && est > 1) est - 1 else est
  }

  /**
   * Generates a B Tree size sequence from total size.
   */
  def generate(n: Long): Seq[Int] = {
    shape(n, height(n))
  }

  def generate2(n: Long): BTreeNode = {
    shape2(n, height(n))
  }
}

case class BTreeNode(root: Int, children: Seq[BTreeNode]) {
  override def toString: String = if (children.nonEmpty) {
    "[" + root.toString + " " + children.map(_.toString).reduce(_ + " " + _) + "]"
  } else {
    root.toString
  }
}
