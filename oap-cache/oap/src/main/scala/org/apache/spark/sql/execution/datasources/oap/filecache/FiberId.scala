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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.parquet.io.SeekableInputStream

import org.apache.spark.sql.execution.datasources.oap.io.DataFile
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.unsafe.Platform

private[oap] abstract class FiberId {
  def toFiberKey(): String = {
    throw new UnsupportedOperationException("Unsupported operation")
  }
}

case class BinaryDataFiberId(file: DataFile, columnIndex: Int, rowGroupId: Int) extends
  DataFiberId {

  private var input: SeekableInputStream = _
  private var offset: Long = _
  private var length: Int = _

  def withLoadCacheParameters(input: SeekableInputStream, offset: Long, length: Int): Unit = {
    this.input = input
    this.offset = offset
    this.length = length
  }

  def cleanLoadCacheParameters(): Unit = {
    input = null
    offset = -1
    length = 0
  }

  override def hashCode(): Int = (file.path + columnIndex + rowGroupId).hashCode

  val fiberKey = s"${file.path}_${rowGroupId}_${columnIndex})"

  override def toFiberKey(): String = fiberKey

  override def equals(obj: Any): Boolean = obj match {
    case another: BinaryDataFiberId =>
      another.columnIndex == columnIndex &&
        another.rowGroupId == rowGroupId &&
        another.file.path.equals(file.path)
    case _ => false
  }

  override def toString: String = {
    s"type: BinaryDataFiber rowGroup: $rowGroupId column: $columnIndex\n\tfile: ${file.path}"
  }

  def doCache(): FiberCache = {
    assert(input != null && offset >= 0 && length > 0,
      "Illegal condition when load binary Fiber to cache.")
    val data = new Array[Byte](length)
    input.seek(offset)
    input.readFully(data)
    val fiber = OapRuntime.getOrCreate.fiberCacheManager.getEmptyDataFiberCache(length)
    Platform.copyMemory(data,
      Platform.BYTE_ARRAY_OFFSET, null, fiber.getBaseOffset, length)
    fiber
  }
}

case class OrcBinaryFiberId(file: DataFile, columnIndex: Int, rowGroupId: Int) extends
  DataFiberId {

  private var input: FSDataInputStream = _
  private var offset: Long = _
  private var length: Int = _

  def withLoadCacheParameters(input: FSDataInputStream, offset: Long, length: Int): Unit = {
    this.input = input
    this.offset = offset
    this.length = length
  }

  def cleanLoadCacheParameters(): Unit = {
    input = null
    offset = -1
    length = 0
  }

  override def hashCode(): Int = (file.path + columnIndex + rowGroupId).hashCode

  val fiberKey = s"${file.path}_${rowGroupId}_${columnIndex})"

  override def toFiberKey(): String = fiberKey

  override def equals(obj: Any): Boolean = obj match {
    case another: OrcBinaryFiberId =>
      another.columnIndex == columnIndex &&
        another.rowGroupId == rowGroupId &&
        another.file.path.equals(file.path)
    case _ => false
  }

  override def toString: String = {
    s"type: ORCColumn rowGroup: $rowGroupId column: $columnIndex\n\tfile: ${file.path}"
  }

  def doCache(): FiberCache = {
    assert(input != null && offset >= 0 && length > 0,
      "Illegal condition when load ORCColumn Fiber to cache.")
    val data = new Array[Byte](length)
    input.readFully((offset), data, 0, data.length);
    val fiber = OapRuntime.getOrCreate.fiberCacheManager.getEmptyDataFiberCache(length)
    Platform.copyMemory(data,
      Platform.BYTE_ARRAY_OFFSET, null, fiber.getBaseOffset, length)
    fiber
  }
}

case class VectorDataFiberId(file: DataFile, columnIndex: Int, rowGroupId: Int) extends
  DataFiberId {

  override def hashCode(): Int = (file.path + columnIndex + rowGroupId).hashCode

  val fiberKey = s"${file.path}_${rowGroupId}_${columnIndex})"

  override def toFiberKey(): String = fiberKey

  override def equals(obj: Any): Boolean = obj match {
    case another: VectorDataFiberId =>
      another.columnIndex == columnIndex &&
        another.rowGroupId == rowGroupId &&
        another.file.path.equals(file.path)
    case _ => false
  }

  override def toString: String = {
    s"type: VectorDataFiber rowGroup: $rowGroupId column: $columnIndex\n\tfile: ${file.path}"
  }
}

private[oap] abstract class DataFiberId extends FiberId {
  def file: DataFile
  def columnIndex: Int
  def rowGroupId: Int
}

private[oap] case class BTreeFiberId(
    getFiberData: () => FiberCache,
    file: String,
    section: Int,
    idx: Int) extends FiberId {

  override def hashCode(): Int = (file + section + idx).hashCode

  val fiberKey = s"${file}_${section}_${idx})"

  override def toFiberKey(): String = fiberKey

  override def equals(obj: Any): Boolean = obj match {
    case another: BTreeFiberId =>
      another.section == section &&
        another.idx == idx &&
        another.file.equals(file)
    case _ => false
  }

  override def toString: String = {
    s"type: BTreeFiber section: $section idx: $idx\n\tfile: $file"
  }
}

private[oap] case class BitmapFiberId(
    getFiberData: () => FiberCache,
    file: String,
    // "0" means no split sections within file.
    sectionIdxOfFile: Int,
    // "0" means no smaller loading units.
    loadUnitIdxOfSection: Int) extends FiberId {

  override def hashCode(): Int = (file + sectionIdxOfFile + loadUnitIdxOfSection).hashCode

  val fiberKey = s"${file}_${sectionIdxOfFile}_${loadUnitIdxOfSection})"

  override def toFiberKey(): String = fiberKey

  override def equals(obj: Any): Boolean = obj match {
    case another: BitmapFiberId =>
      another.sectionIdxOfFile == sectionIdxOfFile &&
        another.loadUnitIdxOfSection == loadUnitIdxOfSection &&
        another.file.equals(file)
    case _ => false
  }

  override def toString: String = {
    s"type: BitmapFiber section: $sectionIdxOfFile idx: $loadUnitIdxOfSection\n\tfile: $file"
  }
}

private[oap] case class TestDataFiberId(getData: () => FiberCache, name: String) extends FiberId {

  override def hashCode(): Int = name.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case another: TestDataFiberId => name.equals(another.name)
    case _ => false
  }

  override def toString: String = {
    s"type: TestDataFiber name: $name"
  }
}

private[oap] case class TestIndexFiberId(getData: () => FiberCache, name: String) extends FiberId {

  override def hashCode(): Int = name.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case another: TestIndexFiberId => name.equals(another.name)
    case _ => false
  }

  override def toString: String = {
    s"type: TestIndexFiber name: $name"
  }
}
