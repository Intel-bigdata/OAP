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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, IntegerType, StructType}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

import scala.collection.immutable.BitSet

class SpinachMetaSuite extends SharedSQLContext with BeforeAndAfter {

  private var tmpDir: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    tmpDir = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tmpDir)
    } finally {
      super.afterAll()
    }
  }

  private def writeMetaFile(path: Path): Unit = {
    val fileHeader = FileHeader(100, 2, 2, Version(1, 0, 0), "FIBER")
    val fileMetas = Array(
      FileMeta("SpinachFile1", 60, "file1"),
      FileMeta("SpinachFile2", 40, "file2"))
    val indexMetas = Array(
      IndexMeta("index1", 1, BitSet.empty + 0),
      IndexMeta("index2", 1, BitSet.empty + 1 + 2))
    val schema = new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType)
    val spinachMeta = SpinachMeta(fileMetas, indexMetas, schema, fileHeader)

    SpinachMeta.write(path, new Configuration(), spinachMeta)
  }

  test("read Spinach Meta") {
    val path = new Path(
      new File(tmpDir.getAbsolutePath, "spinach.meta").getAbsolutePath)
    writeMetaFile(path)

    val spinachMeta = SpinachMeta.initialize(path, new Configuration())
    val fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 100)
    assert(fileHeader.dataFileCount === 2)
    assert(fileHeader.indexCount === 2)
    assert(fileHeader.version === Version(1, 0, 0))
    assert(fileHeader.magicNumber === "FIBER")

    val fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 2)
    assert(fileMetas(0).fingerprint === "SpinachFile1")
    assert(fileMetas(0).dataFileName === "file1")
    assert(fileMetas(0).recordCount === 60)
    assert(fileMetas(1).fingerprint === "SpinachFile2")
    assert(fileMetas(1).dataFileName === "file2")
    assert(fileMetas(1).recordCount === 40)

    val indexMetas = spinachMeta.indexMetas
    assert(indexMetas.length === 2)
    assert(indexMetas(0).name === "index1")
    assert(indexMetas(0).indexType === 1)
    assert(indexMetas(0).key.size === 1)
    assert(indexMetas(0).key(0) === true)
    assert(indexMetas(1).name === "index2")
    assert(indexMetas(1).indexType === 1)
    assert(indexMetas(1).key.size === 2)
    assert(indexMetas(1).key(1) === true)
    assert(indexMetas(1).key(2) === true)

    assert(spinachMeta.schema === new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType))
  }

  test("Empty Spinach Meta") {
    val path = new Path(
      new File(tmpDir.getAbsolutePath, "emptySpinach.meta").getAbsolutePath)
    val fileHeaderToWrite = FileHeader(0, 0, 0, Version(1, 0, 0), "FIBER")
    val spinachMetaToWrite = SpinachMeta(Array.empty, Array.empty, new StructType(), fileHeaderToWrite)
    SpinachMeta.write(path, new Configuration(), spinachMetaToWrite)

    val spinachMeta = SpinachMeta.initialize(path, new Configuration())
    val fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 0)
    assert(fileHeader.dataFileCount === 0)
    assert(fileHeader.indexCount === 0)
    assert(fileHeader.version === Version(1, 0, 0))
    assert(fileHeader.magicNumber === "FIBER")

    assert(spinachMeta.fileMetas.length === 0)
    assert(spinachMeta.indexMetas.length === 0)
    assert(spinachMeta.schema.length === 0)
  }
}
