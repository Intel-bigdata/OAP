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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class BTreeFileReaderWriterSuite extends SparkFunSuite {

  test("BTree File Read/Write") {
    val path = new Path(Utils.createTempDir().getAbsolutePath, "index")
    val configuration = new Configuration()
    val footer = "footer".getBytes("UTF-8")
    val rowIdList = "rowIdList".getBytes("UTF-8")
    val nodes = (0 until 5).map(i => s"node$i".getBytes("UTF-8"))
    // Write content into File
    val writer = BTreeIndexFileWriter(configuration, path)
    writer.start()
    writer.writeNode(nodes.reduce(_ ++ _))
    writer.writeRowIdList(rowIdList)
    writer.writeFooter(footer)
    writer.end()
    writer.close()
    // Read content from File
    val reader = BTreeIndexFileReader(configuration, path)
    val footerRead = reader.readFooter()
    val rowIdListRead = reader.readRowIdList()
    val nodesRead = (0 until 5).map(i =>
      reader.readNode(nodes.slice(0, i).map(_.length).sum, nodes(i).length))
    // Check result
    assert(footer === footerRead)
    assert(rowIdList === rowIdListRead)
    nodes.zip(nodesRead).foreach {
      case (node, nodeRead) => assert(node === nodeRead)
    }
  }
}
