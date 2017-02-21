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
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, Descending, EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}
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
      .addIndexMeta(IndexMeta("index3", BTreeIndex()
        .appendEntry(BTreeIndexEntry(1, Descending))
        .appendEntry(BTreeIndexEntry(0, Ascending))))
      .withNewSchema(new StructType()
        .add("a", IntegerType).add("b", IntegerType).add("c", StringType))
      .withNewDataReaderClassName("NotExistedDataFileClassName")
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
    assert(fileHeader.indexCount === 3)

    val fileMetas = spinachMeta.fileMetas
    assert(fileMetas.length === 2)
    assert(fileMetas(0).fingerprint === "SpinachFile1")
    assert(fileMetas(0).dataFileName === "file1")
    assert(fileMetas(0).recordCount === 60)
    assert(fileMetas(1).fingerprint === "SpinachFile2")
    assert(fileMetas(1).dataFileName === "file2")
    assert(fileMetas(1).recordCount === 40)

    val indexMetas = spinachMeta.indexMetas
    assert(indexMetas.length === 3)
    assert(indexMetas(0).name === "index1")
    assert(indexMetas(0).indexType.isInstanceOf[BTreeIndex])
    val index1 = indexMetas(0).indexType.asInstanceOf[BTreeIndex]
    assert(index1.entries.size === 2)
    assert(index1.entries(0).ordinal === 0)
    assert(index1.entries(0).dir === Descending)
    assert(index1.entries(1).ordinal === 1)
    assert(index1.entries(1).dir === Ascending)
    val index3 = indexMetas(2).indexType.asInstanceOf[BTreeIndex]
    assert(index3.entries.size === 2)
    assert(index3.entries(0).ordinal === 1)
    assert(index3.entries(0).dir === Descending)

    assert(indexMetas(1).name === "index2")
    assert(indexMetas(1).indexType.isInstanceOf[BitMapIndex])
    val index2 = indexMetas(1).indexType.asInstanceOf[BitMapIndex]
    assert(index2.entries.size === 2)
    assert(index2.entries(0) === 1)
    assert(index2.entries(1) === 2)

    assert(spinachMeta.schema === new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType))
    assert(spinachMeta.dataReaderClassName === "NotExistedDataFileClassName")
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
    assert(spinachMeta.dataReaderClassName === SpinachFileFormat.SPINACH_DATA_FILE_CLASSNAME)
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

    sql("create sindex index1 on spnt1 (a)")
    sql("create sindex index2 on spnt1 (a asc)") // dup as index1, still creating
    sql("create sindex index3 on spnt1 (a desc)")
    sql("create sindex if not exists index3 on spnt1 (a desc)") // not creating
    sql("drop sindex index2 on spnt1") // dropping
    sql("drop sindex if exists index5 on spnt1") // not dropping
    sql("drop sindex if exists index2 on spnt1") // not dropping

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
    assert(spinachMeta2.dataReaderClassName === spinachMeta.dataReaderClassName)
  }

  test("Spinach IndexMeta Test") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    df.write.format("spn").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
    val spnDf = sqlContext.read.format("spn").load(tmpDir.getAbsolutePath)
    spnDf.registerTempTable("spnt1")

    val path = new Path(
      new File(tmpDir.getAbsolutePath, SpinachFileFormat.SPINACH_META_FILE).getAbsolutePath)

    sql("create sindex mi on spnt1 (a, c desc, b asc)")

    val spinachMeta = DataSourceMeta.initialize(path, new Configuration())
    val fileHeader = spinachMeta.fileHeader
    assert(fileHeader.indexCount === 1)
    val indexMetas = spinachMeta.indexMetas
    assert(indexMetas.length === 1)
    val indexMeta = indexMetas.head
    assert(indexMeta.name === "mi")
    assert(indexMeta.indexType === BTreeIndex(Seq(
      BTreeIndexEntry(0, Ascending),
      BTreeIndexEntry(2, Descending),
      BTreeIndexEntry(1, Ascending)
    )))
  }

  test("Spinach Meta integration test for parquet") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
    val spnDf = sqlContext.read.format("parquet").load(tmpDir.getAbsolutePath)
    spnDf.registerTempTable("spnt1")

    val path = new Path(
      new File(tmpDir.getAbsolutePath, SpinachFileFormat.SPINACH_META_FILE).getAbsolutePath)

    val fs = path.getFileSystem(new Configuration())
    assert(!fs.exists(path))

    sql("create sindex index1 on spnt1 (a)")
    sql("create sindex index2 on spnt1 (a asc)") // dup as index1, still creating
    sql("create sindex index3 on spnt1 (a desc)")
    sql("create sindex if not exists index3 on spnt1 (a desc)") // not creating
    sql("drop sindex index2 on spnt1") // dropping
    sql("drop sindex if exists index5 on spnt1") // not dropping
    sql("drop sindex if exists index2 on spnt1") // not dropping

    val spinachMeta2 = DataSourceMeta.initialize(path, new Configuration())
    val fileHeader2 = spinachMeta2.fileHeader
    assert(fileHeader2.recordCount === 100)
    assert(fileHeader2.dataFileCount === 3)
    assert(fileHeader2.indexCount === 2)
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
    assert(spinachMeta.dataReaderClassName === SpinachFileFormat.SPINACH_DATA_FILE_CLASSNAME)


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

  test("test hasAvailableIndex from Meta") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
    val spnDf = sqlContext.read.format("parquet").load(tmpDir.getAbsolutePath)
    spnDf.createOrReplaceTempView("spnt1")

    sql("create sindex index1 on spnt1 (a)")

    val path = new Path(
      new File(tmpDir.getAbsolutePath, SpinachFileFormat.SPINACH_META_FILE).getAbsolutePath)
    val meta = DataSourceMeta.initialize(path, new Configuration())

    val eq = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)))
    val eq2 = Seq(EqualTo(AttributeReference("b", IntegerType)(), Literal(1)))
    val lt = Seq(LessThan(AttributeReference("a", IntegerType)(), Literal(1)))
    val gt = Seq(GreaterThan(AttributeReference("a", IntegerType)(), Literal(1)))
    val lte = Seq(LessThanOrEqual(AttributeReference("a", IntegerType)(), Literal(1)))
    val gte = Seq(GreaterThanOrEqual(AttributeReference("a", IntegerType)(), Literal(1)))

    assert(meta.hasAvailableIndex(eq) == true)
    assert(meta.hasAvailableIndex(lt) == true)
    assert(meta.hasAvailableIndex(gt) == true)
    assert(meta.hasAvailableIndex(lte) == true)
    assert(meta.hasAvailableIndex(gte) == true)
    assert(meta.hasAvailableIndex(eq2) == false)

  }
}
