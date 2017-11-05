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

import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.collection.mutable.ArrayBuffer

// Trie Node
private[oap] trait TrieNode {
  def nodeKey: Byte
  def childCount: Int
  def childAt(idx: Int): TrieNode
  def children: Seq[TrieNode]
  def canTerminate: Boolean
  def rowIdsPointer: Int
  def allPointers: Seq[Int]
  // for debug
  override def toString: String = {
    def disp(key: Byte): String =
      if (key >= 32 && key <= 126) key.toChar.toString else "\\" + key.toLong.toHexString
    if (childCount > 0) {
      s"[Trie(${disp(nodeKey)},$rowIdsPointer) ${children.mkString(" ")}]"
    } else {
      s"Trie(${disp(nodeKey)},$rowIdsPointer)"
    }
  }
}

/**
 * A File-based Trie node. The first int is combined of node Key (1-byte, this is because we
 * are using UTF-8 encoding in spark) and child count (2-byte, a total of 4-byte since it's up
 * to 256 and align to 4) of the trie node, then an int indicating rowIdsPointer of this node,
 * -1 if there's no options. Then [child count] * [child offset of file(int, 4 bytes)] is written.
 * Note those children should be ordered in ascii ascending order to enable bisection method.
 * @param buffer the total area of data
 * @param intOffset offset of this node in the file stored
 */
private[oap] case class UnsafeTrie(
    buffer: ChunkedByteBuffer,
    page: Int,
    intOffset: Int,
    dataEnd: Long) extends TrieNode with UnsafeIndexTree {
  def offset: Long = intOffset.toLong
  private lazy val firstInt: Int = Platform.getInt(baseObj, baseOffset + offset)
  // def nodeKey: Byte = Platform.getShort(baseObj, baseOffset + offset).toByte
  def nodeKey: Byte = (firstInt >> 16).toByte
  // def childCount: Int = Platform.getShort(baseObj, baseOffset + offset + 2)
  def childCount: Int = firstInt & 0xFFFF
  def canTerminate: Boolean = rowIdsPointer >= 0
  def rowIdsPointer: Int = Platform.getInt(baseObj, baseOffset + offset + 4)
  def children: Seq[TrieNode] = (0 until childCount).map(childAt)
  def childAt(idx: Int): TrieNode =
    UnsafeTrie(buffer,
      Platform.getInt(baseObj, baseOffset + offset + 8 + idx * 8 + 4),
      Platform.getInt(baseObj, baseOffset + offset + 8 + idx * 8), dataEnd)
  def allPointers: Seq[Int] = if (canTerminate) {
    rowIdsPointer +: children.flatMap(_.allPointers)
  } else {
    children.flatMap(_.allPointers)
  }
  override def length: Int = 8 + 8 * childCount
  override val baseOffset = super.baseOffset + dataEnd
}

private[oap] case class UnsafeIds(buffer: ChunkedByteBuffer, offset: Long) extends UnsafeIndexTree {
  def count: Int = Platform.getInt(baseObj, baseOffset + offset)
  def apply(id: Int): Int = {
    assert(id < count && id >= 0, id)
    Platform.getInt(baseObj, baseOffset + offset + 4 * id + 4)
  }
}

case class TriePointer(page: Int, offset: Int)

private[oap] case class InMemoryTrie(
    nodeKey: Byte = 0,
    var rowIdsPointer: Int = -1) extends TrieNode {
  val innerChildren: ArrayBuffer[TrieNode] = new ArrayBuffer()
  def childAt(idx: Int): TrieNode = children(idx)
  def allPointers: Seq[Int] = Nil
  def addChild(node: TrieNode): InMemoryTrie = {
    innerChildren += node
    this
  }
  def children: Seq[TrieNode] = {
    innerChildren.sortBy(_.nodeKey)
  }
  def setPointer(value: Int): InMemoryTrie = {
    rowIdsPointer = value
    this
  }
  def canTerminate: Boolean = rowIdsPointer >= 0
  def childCount: Int = innerChildren.size
}
