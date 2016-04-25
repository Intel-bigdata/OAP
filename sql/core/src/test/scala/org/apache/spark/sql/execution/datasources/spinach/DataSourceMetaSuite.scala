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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

class DataSourceMetaSuite extends SharedSQLContext with BeforeAndAfter {

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
    val spinachMeta = DataSourceMeta.newBuilder()
      .addFileMeta(FileMeta("SpinachFile1", 60, "file1"))
      .addFileMeta(FileMeta("SpinachFile2", 40, "file2"))
      .addIndexMeta(IndexMeta("index1", BTreeIndex()
        .appendEntry(BTreeIndexEntry(0, Descending))
        .appendEntry(BTreeIndexEntry(1, Ascending))))
      .addIndexMeta(IndexMeta("index2", BitMapIndex()
        .appendEntry(1)
        .appendEntry(2)))
      .withNewSchema(new StructType()
        .add("a", IntegerType).add("b", IntegerType).add("c", StringType))
      .build()

    DataSourceMeta.write(path, new Configuration(), spinachMeta)
  }

  test("read Spinach Meta") {
    val path = new Path(
      new File(tmpDir.getAbsolutePath, "spinach.meta").getAbsolutePath)
    writeMetaFile(path)

    val spinachMeta = DataSourceMeta.initialize(path, new Configuration())
    val fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 100)
    assert(fileHeader.dataFileCount === 2)
    assert(fileHeader.indexCount === 2)

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
    assert(indexMetas(0).indexType.isInstanceOf[BTreeIndex])
    val index1 = indexMetas(0).indexType.asInstanceOf[BTreeIndex]
    assert(index1.entries.size === 2)
    assert(index1.entries(0).ordinal === 0)
    assert(index1.entries(0).dir === Descending)
    assert(index1.entries(1).ordinal === 1)
    assert(index1.entries(1).dir === Ascending)

    assert(indexMetas(1).name === "index2")
    assert(indexMetas(1).indexType.isInstanceOf[BitMapIndex])
    val index2 = indexMetas(1).indexType.asInstanceOf[BitMapIndex]
    assert(index2.entries.size === 2)
    assert(index2.entries(0) === 1)
    assert(index2.entries(1) === 2)

    assert(spinachMeta.schema === new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType))
  }

  test("read empty Spinach Meta") {
    val path = new Path(
      new File(tmpDir.getAbsolutePath, "emptySpinach.meta").getAbsolutePath)
    DataSourceMeta.write(path, new Configuration(), DataSourceMeta.newBuilder().build())

    val spinachMeta = DataSourceMeta.initialize(path, new Configuration())
    val fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 0)
    assert(fileHeader.dataFileCount === 0)
    assert(fileHeader.indexCount === 0)

    assert(spinachMeta.fileMetas.length === 0)
    assert(spinachMeta.indexMetas.length === 0)
    assert(spinachMeta.schema.length === 0)
  }
}
