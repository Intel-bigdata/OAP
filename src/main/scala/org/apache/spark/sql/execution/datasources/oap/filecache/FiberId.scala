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

import org.apache.spark.sql.execution.datasources.oap.io.DataFile

private[oap] abstract class FiberId {}

private[oap] case class DataFiberId(file: DataFile, columnIndex: Int, rowGroupId: Int) extends
    FiberId {

  override def hashCode(): Int = (file.path + columnIndex + rowGroupId).hashCode

  override def equals(obj: Any): Boolean = obj match {
    case another: DataFiberId =>
      another.columnIndex == columnIndex &&
        another.rowGroupId == rowGroupId &&
        another.file.path.equals(file.path)
    case _ => false
  }

  override def toString: String = {
    s"type: DataFiber rowGroup: $rowGroupId column: $columnIndex\n\tfile: ${file.path}"
  }
}

private[oap] case class BTreeFiberId(
    getFiberData: () => FiberCache,
    file: String,
    section: Int,
    idx: Int) extends FiberId {

  override def hashCode(): Int = (file + section + idx).hashCode

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

private[oap] case class TestFiberId(getData: () => FiberCache, name: String) extends FiberId {

  override def hashCode(): Int = name.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case another: TestFiberId => name.equals(another.name)
    case _ => false
  }

  override def toString: String = {
    s"type: TestFiber name: $name"
  }
}
