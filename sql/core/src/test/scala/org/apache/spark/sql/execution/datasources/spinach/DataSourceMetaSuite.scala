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
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.util.Utils

class DataSourceMetaSuite extends SharedSQLContext with BeforeAndAfter {
  import testImplicits._
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
      new File(tmpDir.getAbsolutePath, "testSpinach.meta").getAbsolutePath)
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

  test("Spinach Meta integration test") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    df.write.format("spn").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
    val spnDf = sqlContext.read.format("spn").load(tmpDir.getAbsolutePath)
    spnDf.registerTempTable("spnt1")

    val path = new Path(
      new File(tmpDir.getAbsolutePath, SpinachFileFormat.SPINACH_META_FILE).getAbsolutePath)
    val spinachMeta = DataSourceMeta.initialize(path, new Configuration())

    val fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 100)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    val fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 100)
    assert(fileMetas(0).dataFileName.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))

    assert(spinachMeta.schema === df.schema)

    sql("create index index1 on spnt1 (a) using btree")
    sql("create index index2 on spnt1 (a asc)") // dup as index1, still creating
    sql("create index index3 on spnt1 (a desc)")
    sql("create index if not exists index3 on spnt1 (a desc)") // not creating
    sql("drop index index2") // dropping
    sql("drop index if exists index5") // not dropping
    sql("drop index if exists index2") // not dropping

    val spinachMeta2 = DataSourceMeta.initialize(path, new Configuration())
    val fileHeader2 = spinachMeta2.fileHeader
    assert(fileHeader2.recordCount === 100)
    assert(fileHeader2.dataFileCount === 3)
    assert(fileHeader2.indexCount === 2)
    // other should keep the same
    val fileMetas2 = spinachMeta2.fileMetas
    assert(fileMetas2.length === 3)
    assert(fileMetas2.map(_.recordCount).sum === 100)
    assert(fileMetas2(0).dataFileName.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))
    assert(spinachMeta2.schema === spinachMeta.schema)
  }

  test("Spinach meta for partitioned table") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, s"row $i", 2015 + i % 2))
      .toDF("id", "name", "year")
    df.write.format("spn")
      .partitionBy("year")
      .mode(SaveMode.Overwrite)
      .save(tmpDir.getAbsolutePath)

    var path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015"), SpinachFileFormat.SPINACH_META_FILE)
    var spinachMeta = DataSourceMeta.initialize(path, new Configuration())

    var fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 50)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    var fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 50)
    assert(fileMetas(0).dataFileName.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))

    path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2016"), SpinachFileFormat.SPINACH_META_FILE)
    spinachMeta = DataSourceMeta.initialize(path, new Configuration())

    fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 50)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 50)
    assert(fileMetas(0).dataFileName.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))


    val readDf = sqlContext.read.format("spn").load(tmpDir.getAbsolutePath)
    assert(readDf.schema === new StructType()
      .add("id", IntegerType).add("name", StringType).add("year", IntegerType))
    val data = readDf.collect().sortBy(_.getInt(0)) // sort locally
    assert(data.length === 100)
    assert(data(0) === Row(1, "row 1", 2016))
    assert(data(1) === Row(2, "row 2", 2015))
    assert(data(99) === Row(100, "row 100", 2015))
  }

  test("Spinach meta for two columns partitioned table") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, s"row $i", 2015 + i % 2, 1 + i % 3))
      .toDF("id", "name", "year", "month")
    df.write.format("spn")
      .partitionBy("year", "month")
      .mode(SaveMode.Overwrite)
      .save(tmpDir.getAbsolutePath)

    var path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015/month=1"), SpinachFileFormat.SPINACH_META_FILE)
    var spinachMeta = DataSourceMeta.initialize(path, new Configuration())

    var fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 16)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    var fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 16)

    path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015/month=2"), SpinachFileFormat.SPINACH_META_FILE)
    spinachMeta = DataSourceMeta.initialize(path, new Configuration())

    fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 17)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 17)
    assert(fileMetas(0).dataFileName.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))

    path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015/month=3"), SpinachFileFormat.SPINACH_META_FILE)
    spinachMeta = DataSourceMeta.initialize(path, new Configuration())

    fileHeader = spinachMeta.fileHeader
    assert(fileHeader.recordCount === 17)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 17)
    assert(fileMetas(0).dataFileName.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))
  }
}
