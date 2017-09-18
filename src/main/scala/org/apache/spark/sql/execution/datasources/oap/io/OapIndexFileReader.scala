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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.spark.sql.execution.datasources.oap.Key

case class IndexTreeMeta(offset: Long, rowCount: Long, key: Key)
case class IndexHeader(children: Seq[IndexTreeMeta])

case class IndexNode(key: Key, children: Seq[IndexNode])
case class IndexTree(root: IndexNode)
case class IndexRowListData(data: Array[Byte])

// OapIndexFileReader is used to read index data from Disk. Also, it will cache the data for
// future use. Index File contains Header, Data and Other parts.
// Header contains the necessary info for index selection and partial loading
// Data is the real index data to locate the Row ID for specific Key
// Other part currently only for BTREE, contains the whole [Key, Seq[RowID]] list.
class OapIndexFileReader {

  // Read index file header from Disk
  def readIndexHeader(): IndexHeader = null

  // Read index row list data from Disk (For BTREE)
  def readIndexRowListData(): IndexRowListData = null

  // Get the index data fiber from Child Tree (One Fiber is One Node)
  // If the fiber hasn't be load to memory, readIndexTree will be called
  def readIndexDataFiber(): IndexNode = null

  // Read the whole child tree from Disk
  private def readIndexTree(TreeId: Int): IndexTree = null
}
