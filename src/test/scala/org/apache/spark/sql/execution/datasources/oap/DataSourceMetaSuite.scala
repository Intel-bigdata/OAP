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

package org.apache.spark.sql.execution.datasources.oap

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.Utils

class DataSourceMetaSuite extends SharedOapContext with BeforeAndAfter {
  import testImplicits._
  private var tmpDir: File = _

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

  // Override afterEach because we don't want to check open streams
  override def beforeEach(): Unit = {}
  override def afterEach(): Unit = {}

  private def writeMetaFile(path: Path): Unit = {
    val schema = new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType)
    writeMetaFile(path: Path, schema, Version.latestVersion())
  }

  private def writeMetaFile(path: Path, schema: StructType, version: Version): Unit = {
    val oapMeta = DataSourceMeta.newBuilder()
      .addFileMeta(FileMeta("OapFile1", 60, "file1"))
      .addFileMeta(FileMeta("OapFile2", 40, "file2"))
      .addIndexMeta(IndexMeta("index1", "15cc47fb3d8", BTreeIndex()
        .appendEntry(BTreeIndexEntry(0, Descending))
        .appendEntry(BTreeIndexEntry(1, Ascending))))
      .addIndexMeta(IndexMeta("index2", "15cc47fb3d9", BitMapIndex()
        .appendEntry(1)
        .appendEntry(2)))
      .addIndexMeta(IndexMeta("index3", "15cc47fb3e0", BTreeIndex()
        .appendEntry(BTreeIndexEntry(1, Descending))
        .appendEntry(BTreeIndexEntry(0, Ascending))))
      .withNewSchema(schema)
      .withNewVersion(version)
      .withNewDataReaderClassName("NotExistedDataFileClassName")
      .build()

    DataSourceMeta.write(path, new Configuration(), oapMeta)
  }

  test("read Oap Meta") {
    val path = new Path(
      new File(tmpDir.getAbsolutePath, "testOap.meta").getAbsolutePath)
    writeMetaFile(path)

    val oapMeta = DataSourceMeta.initialize(path, new Configuration())
    val fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 100)
    assert(fileHeader.dataFileCount === 2)
    assert(fileHeader.indexCount === 3)

    val fileMetas = oapMeta.fileMetas
    assert(fileMetas.length === 2)
    assert(fileMetas(0).fingerprint === "OapFile1")
    assert(fileMetas(0).dataFileName === "file1")
    assert(fileMetas(0).recordCount === 60)
    assert(fileMetas(1).fingerprint === "OapFile2")
    assert(fileMetas(1).dataFileName === "file2")
    assert(fileMetas(1).recordCount === 40)

    val indexMetas = oapMeta.indexMetas
    assert(indexMetas.length === 3)
    assert(indexMetas(0).name === "index1")
    assert(indexMetas(0).time === "15cc47fb3d8")
    assert(indexMetas(0).indexType.isInstanceOf[BTreeIndex])
    val index1 = indexMetas(0).indexType.asInstanceOf[BTreeIndex]
    assert(index1.entries.size === 2)
    assert(index1.entries.head.ordinal === 0)
    assert(index1.entries.head.dir === Descending)
    assert(index1.entries(1).ordinal === 1)
    assert(index1.entries(1).dir === Ascending)
    val index3 = indexMetas(2).indexType.asInstanceOf[BTreeIndex]
    assert(index3.entries.size === 2)
    assert(index3.entries.head.ordinal === 1)
    assert(index3.entries.head.dir === Descending)

    assert(indexMetas(1).name === "index2")
    assert(indexMetas(1).time === "15cc47fb3d9")
    assert(indexMetas(1).indexType.isInstanceOf[BitMapIndex])
    val index2 = indexMetas(1).indexType.asInstanceOf[BitMapIndex]
    assert(index2.entries.size === 2)
    assert(index2.entries.head === 1)
    assert(index2.entries(1) === 2)

    assert(oapMeta.schema === new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType))
    assert(oapMeta.dataReaderClassName === "NotExistedDataFileClassName")
  }

  test("read empty Oap Meta") {
    val path = new Path(
      new File(tmpDir.getAbsolutePath, "emptyOap.meta").getAbsolutePath)
    DataSourceMeta.write(path, new Configuration(), DataSourceMeta.newBuilder().build())

    val oapMeta = DataSourceMeta.initialize(path, new Configuration())
    val fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 0)
    assert(fileHeader.dataFileCount === 0)
    assert(fileHeader.indexCount === 0)

    assert(oapMeta.fileMetas.length === 0)
    assert(oapMeta.indexMetas.length === 0)
    assert(oapMeta.schema.length === 0)
    assert(oapMeta.dataReaderClassName === OapFileFormat.OAP_DATA_FILE_CLASSNAME)
  }

  test("Oap Meta integration test") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    df.write.format("oap").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
    val oapDf = sqlContext.read.format("oap").load(tmpDir.getAbsolutePath)
    oapDf.createOrReplaceTempView("oapt1")

    val path = new Path(
      new File(tmpDir.getAbsolutePath, OapFileFormat.OAP_META_FILE).getAbsolutePath)
    val oapMeta = DataSourceMeta.initialize(path, new Configuration())

    val fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 100)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    val fileMetas = oapMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 100)
    assert(fileMetas(0).dataFileName.endsWith(OapFileFormat.OAP_DATA_EXTENSION))

    assert(oapMeta.schema === df.schema)
    withIndex(TestIndex("oapt1", "index1"),
      TestIndex("oapt1", "index3")) {
      withIndex(TestIndex("oapt1", "index2")) {
        sql("create oindex index1 on oapt1 (a)")
        sql("create oindex index2 on oapt1 (a asc)") // dup as index1, still creating
        sql("create oindex index3 on oapt1 (a desc)")
        sql("create oindex if not exists index3 on oapt1 (a desc)") // not creating
      }
      sql("drop oindex if exists index5 on oapt1") // not dropping
      sql("drop oindex if exists index2 on oapt1") // not dropping

      val oapMeta2 = DataSourceMeta.initialize(path, new Configuration())
      val fileHeader2 = oapMeta2.fileHeader
      assert(fileHeader2.recordCount === 100)
      assert(fileHeader2.dataFileCount === 3)
      assert(fileHeader2.indexCount === 2)
      // other should keep the same
      val fileMetas2 = oapMeta2.fileMetas
      assert(fileMetas2.length === 3)
      assert(fileMetas2.map(_.recordCount).sum === 100)
      assert(fileMetas2(0).dataFileName.endsWith(OapFileFormat.OAP_DATA_EXTENSION))
      assert(oapMeta2.schema === oapMeta.schema)
      assert(oapMeta2.dataReaderClassName === oapMeta.dataReaderClassName)
    }
  }

  test("Oap IndexMeta Test") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    df.write.format("oap").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
    val oapDf = sqlContext.read.format("oap").load(tmpDir.getAbsolutePath)
    oapDf.createOrReplaceTempView("oapt1")

    val path = new Path(
      new File(tmpDir.getAbsolutePath, OapFileFormat.OAP_META_FILE).getAbsolutePath)
    withIndex(TestIndex("oapt1", "mi")) {
      sql("create oindex mi on oapt1 (a, c desc, b asc)")

      val oapMeta = DataSourceMeta.initialize(path, new Configuration())
      val fileHeader = oapMeta.fileHeader
      assert(fileHeader.indexCount === 1)
      val indexMetas = oapMeta.indexMetas
      assert(indexMetas.length === 1)
      val indexMeta = indexMetas.head
      assert(indexMeta.name === "mi")
      assert(indexMeta.indexType === BTreeIndex(Seq(
        BTreeIndexEntry(0, Ascending),
        BTreeIndexEntry(2, Descending),
        BTreeIndexEntry(1, Ascending)
      )))
    }
  }

  test("Oap Meta integration test for parquet and orc") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    val fileFormats = Array("parquet", "orc")
    fileFormats.foreach(fileFormat => {
      df.write.format(s"${fileFormat}").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
      val oapDf = sqlContext.read.format(s"${fileFormat}").load(tmpDir.getAbsolutePath)
      oapDf.createOrReplaceTempView("oapt1")

      val path = new Path(
        new File(tmpDir.getAbsolutePath, OapFileFormat.OAP_META_FILE).getAbsolutePath)

      val fs = path.getFileSystem(new Configuration())
      assert(!fs.exists(path))
      withIndex(TestIndex("oapt1", "index1"),
        TestIndex("oapt1", "index3")) {
        withIndex(TestIndex("oapt1", "index2")) {
          sql("create oindex index1 on oapt1 (a)")
          sql("create oindex index2 on oapt1 (a asc)") // dup as index1, still creating
          sql("create oindex index3 on oapt1 (a desc)")
          sql("create oindex if not exists index3 on oapt1 (a desc)") // not creating
        }
        sql("drop oindex if exists index5 on oapt1") // not dropping
        sql("drop oindex if exists index2 on oapt1") // not dropping

        val oapMeta2 = DataSourceMeta.initialize(path, new Configuration())
        val fileHeader2 = oapMeta2.fileHeader
        assert(fileHeader2.recordCount === 100)
        assert(fileHeader2.dataFileCount === 3)
        assert(fileHeader2.indexCount === 2)

        // Append more data to test refresh cmd.
        df.write.format(s"$fileFormat").mode(SaveMode.Append).save(tmpDir.getAbsolutePath)
        val oapDf2 = sqlContext.read.format(s"$fileFormat").load(tmpDir.getAbsolutePath)
        oapDf2.createOrReplaceTempView("oapt1")
        sql("refresh oindex on oapt1")
        val oapMeta3 = DataSourceMeta.initialize(path, new Configuration())
        val fileHeader3 = oapMeta3.fileHeader
        assert(fileHeader3.recordCount === 200)
        assert(fileHeader3.dataFileCount === 6)
        assert(fileHeader3.indexCount === 2)
      }
    })
  }

  test("FileMeta's data file name test for parquet and orc") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    val fileFormats = Array("parquet", "orc")
    fileFormats.foreach(fileFormat => {
      df.write.format(s"${fileFormat}").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
      val oapDf = sqlContext.read.format(s"${fileFormat}").load(tmpDir.getAbsolutePath)
      oapDf.createOrReplaceTempView("t")

      val path = new Path(
        new File(tmpDir.getAbsolutePath, OapFileFormat.OAP_META_FILE).getAbsolutePath)

      val fs = path.getFileSystem(new Configuration())
      assert(!fs.exists(path))

      withIndex(TestIndex("t", "index1")) {
        sql("create oindex index1 on t (a)") // this will create index files along with meta file
        assert(fs.exists(path))

        val oapMeta = DataSourceMeta.initialize(path, sparkContext.hadoopConfiguration)
        val fileMetas = oapMeta.fileMetas
        assert(fileMetas.length === 3)
        assert(fileMetas.map(_.recordCount).sum === 100)
        assert(fileMetas(0).dataFileName.endsWith("." + s"${fileFormat}"))
        assert(fileMetas(0).dataFileName.startsWith("part"))

        // Append more data to test refresh cmd.
        df.write.format(s"$fileFormat").mode(SaveMode.Append).save(tmpDir.getAbsolutePath)
        val oapDf2 = sqlContext.read.format(s"$fileFormat").load(tmpDir.getAbsolutePath)
        oapDf2.createOrReplaceTempView("t")
        sql("refresh oindex on t")
        val oapMeta1 = DataSourceMeta.initialize(path, sparkContext.hadoopConfiguration)
        val fileMetas1 = oapMeta1.fileMetas
        assert(fileMetas1.length === 6)
        assert(fileMetas1.map(_.recordCount).sum === 200)
        assert(fileMetas1(0).dataFileName.endsWith("." + s"$fileFormat"))
        assert(fileMetas1(0).dataFileName.startsWith("part"))
      }
    })
  }

  test("Oap meta for partitioned table") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, s"row $i", 2015 + i % 2))
      .toDF("id", "name", "year")
    df.write.format("oap")
      .partitionBy("year")
      .mode(SaveMode.Overwrite)
      .save(tmpDir.getAbsolutePath)

    var path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015"), OapFileFormat.OAP_META_FILE)
    var oapMeta = DataSourceMeta.initialize(path, new Configuration())

    var fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 50)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    var fileMetas = oapMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 50)
    assert(fileMetas(0).dataFileName.endsWith(OapFileFormat.OAP_DATA_EXTENSION))

    path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2016"), OapFileFormat.OAP_META_FILE)
    oapMeta = DataSourceMeta.initialize(path, new Configuration())

    fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 50)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    fileMetas = oapMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 50)
    assert(fileMetas(0).dataFileName.endsWith(OapFileFormat.OAP_DATA_EXTENSION))
    assert(oapMeta.dataReaderClassName === OapFileFormat.OAP_DATA_FILE_CLASSNAME)


    val readDf = sqlContext.read.format("oap").load(tmpDir.getAbsolutePath)
    assert(readDf.schema === new StructType()
      .add("id", IntegerType).add("name", StringType).add("year", IntegerType))
    val data = readDf.collect().sortBy(_.getInt(0)) // sort locally
    assert(data.length === 100)
    assert(data(0) === Row(1, "row 1", 2016))
    assert(data(1) === Row(2, "row 2", 2015))
    assert(data(99) === Row(100, "row 100", 2015))
  }

  test("Oap meta for two columns partitioned table") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, s"row $i", 2015 + i % 2, 1 + i % 3))
      .toDF("id", "name", "year", "month")
    df.write.format("oap")
      .partitionBy("year", "month")
      .mode(SaveMode.Overwrite)
      .save(tmpDir.getAbsolutePath)

    var path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015/month=1"), OapFileFormat.OAP_META_FILE)
    var oapMeta = DataSourceMeta.initialize(path, new Configuration())

    var fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 16)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    var fileMetas = oapMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 16)

    path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015/month=2"), OapFileFormat.OAP_META_FILE)
    oapMeta = DataSourceMeta.initialize(path, new Configuration())

    fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 17)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    fileMetas = oapMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 17)
    assert(fileMetas(0).dataFileName.endsWith(OapFileFormat.OAP_DATA_EXTENSION))

    path = new Path(
      new Path(tmpDir.getAbsolutePath, "year=2015/month=3"), OapFileFormat.OAP_META_FILE)
    oapMeta = DataSourceMeta.initialize(path, new Configuration())

    fileHeader = oapMeta.fileHeader
    assert(fileHeader.recordCount === 17)
    assert(fileHeader.dataFileCount === 3)
    assert(fileHeader.indexCount === 0)

    fileMetas = oapMeta.fileMetas
    assert(fileMetas.length === 3)
    assert(fileMetas.map(_.recordCount).sum === 17)
    assert(fileMetas(0).dataFileName.endsWith(OapFileFormat.OAP_DATA_EXTENSION))
  }

  test("test hasAvailableIndex from Meta") {
    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(tmpDir.getAbsolutePath)
    val oapDf = sqlContext.read.format("parquet").load(tmpDir.getAbsolutePath)
    oapDf.createOrReplaceTempView("oapt1")
    withIndex(TestIndex("oapt1", "indexA"),
      TestIndex("oapt1", "indexC"),
      TestIndex("oapt1", "indexABC")) {
      sql("create oindex indexA on oapt1 (a)")
      sql("create oindex indexC on oapt1 (c) using bitmap")
      sql("create oindex indexABC on oapt1 (a,b,c)")

      val path = new Path(
        new File(tmpDir.getAbsolutePath, OapFileFormat.OAP_META_FILE).getAbsolutePath)
      val meta = DataSourceMeta.initialize(path, new Configuration())

      val bTreeIndexAttrSet = new mutable.HashSet[String]()
      val bitmapIndexAttrSet = new mutable.HashSet[String]()
      for (idxMeta <- meta.indexMetas) {
        idxMeta.indexType match {
          case BTreeIndex(entries) =>
            bTreeIndexAttrSet.add(meta.schema(entries.head.ordinal).name)
          case BitMapIndex(entries) =>
            entries.map(ordinal => meta.schema(ordinal).name).foreach(bitmapIndexAttrSet.add)
          case _ => // we don't support other types of index
        }
      }
      val hashSetList = new mutable.ListBuffer[mutable.HashSet[String]]()
      hashSetList.append(bTreeIndexAttrSet)
      hashSetList.append(bitmapIndexAttrSet)

      // Query "select * from t where b == 1" will generate IsNotNull expr which is unsupportted
      val isNotNull = Seq(IsNotNull(AttributeReference("b", IntegerType)()),
        IsNotNull(AttributeReference("a", IntegerType)()))
      val eq = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)))
      val eq2 = Seq(EqualTo(AttributeReference("b", IntegerType)(), Literal(1)))
      val eq3 = Seq(EqualTo(Literal(1), AttributeReference("a", IntegerType)()))
      val lt = Seq(LessThan(AttributeReference("a", IntegerType)(), Literal(1)))
      val gt = Seq(GreaterThan(AttributeReference("a", IntegerType)(), Literal(1)))
      val gt2 = Seq(GreaterThan(AttributeReference("c", StringType)(), Literal("A Row")))
      val lte = Seq(LessThanOrEqual(AttributeReference("a", IntegerType)(), Literal(1)))
      val gte = Seq(GreaterThanOrEqual(AttributeReference("a", IntegerType)(), Literal(1)))
      val gte1 = Seq(GreaterThanOrEqual(Literal(1), AttributeReference("a", IntegerType)()))
      val inSet = Seq(InSet(AttributeReference("a", IntegerType)(),
        Set(Literal(1), Literal(2), Literal(3), Literal(4), Literal(5), Literal(6),
          Literal(7), Literal(8), Literal(9), Literal(10), Literal(11))))
      val or1 = Seq(Or(GreaterThan(AttributeReference("a", IntegerType)(), Literal(15)),
        EqualTo(AttributeReference("a", IntegerType)(), Literal(1))))
      val or2 = Seq(Or(GreaterThan(AttributeReference("a", IntegerType)(), Literal(15)),
        LessThan(AttributeReference("a", IntegerType)(), Literal(3))))

      val or3 = Seq(Or(GreaterThan(AttributeReference("c", StringType)(), Literal("A row")),
        LessThan(AttributeReference("c", StringType)(), Literal("A row"))))

      val or4 = Seq(Or(GreaterThan(AttributeReference("a", IntegerType)(), Literal(3)),
        LessThan(AttributeReference("c", StringType)(), Literal("A row"))))

      val complicated_Or = Seq(Or(GreaterThan(AttributeReference("a", IntegerType)(), Literal(50)),
        And(GreaterThan(AttributeReference("a", IntegerType)(), Literal(3)),
          LessThan(AttributeReference("a", IntegerType)(), Literal(24)))))

      val moreComplicated_Or =
        Seq(Or(And(GreaterThan(AttributeReference("a", IntegerType)(), Literal(56)),
          LessThan(AttributeReference("a", IntegerType)(), Literal(97))),
          And(GreaterThan(AttributeReference("a", IntegerType)(), Literal(3)),
            LessThan(AttributeReference("a", IntegerType)(), Literal(24)))))

      // ************** Muliti-Column Search Queries**********
      val multi_column1 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        EqualTo(AttributeReference("b", IntegerType)(), Literal(56)),
        LessThan(AttributeReference("c", StringType)(), Literal("A row")))

      val multi_column2 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        GreaterThan(AttributeReference("b", IntegerType)(), Literal(56)),
        LessThan(AttributeReference("c", StringType)(), Literal("A row")))

      val multi_column3 = Seq(GreaterThan(AttributeReference("a", IntegerType)(), Literal(1)),
        GreaterThan(AttributeReference("b", IntegerType)(), Literal(56)),
        LessThan(AttributeReference("c", StringType)(), Literal("A row")))

      val multi_column4 = Seq(LessThan(AttributeReference("a", IntegerType)(), Literal(1)),
        GreaterThan(AttributeReference("b", IntegerType)(), Literal(56)),
        LessThan(AttributeReference("c", StringType)(), Literal("A row")))

      val multi_column5 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        LessThan(AttributeReference("c", StringType)(), Literal("A row")))

      val multi_column6 = Seq(GreaterThan(AttributeReference("a", IntegerType)(), Literal(1)),
        LessThan(AttributeReference("c", StringType)(), Literal("A row")))

      val multi_column7 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        GreaterThan(AttributeReference("b", IntegerType)(), Literal(56)))

      val multi_column8 = Seq(LessThan(AttributeReference("a", IntegerType)(), Literal(1)),
        GreaterThan(AttributeReference("b", IntegerType)(), Literal(56)))

      val multi_column9 = Seq(EqualTo(AttributeReference("b", IntegerType)(), Literal(1)),
        GreaterThan(AttributeReference("c", StringType)(), Literal("a Row")))

      val multi_column10 = Seq(LessThan(AttributeReference("b", IntegerType)(), Literal(1)),
        GreaterThan(AttributeReference("c", StringType)(), Literal("a Row")))

      val multi_column11 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        EqualTo(AttributeReference("b", IntegerType)(), Literal(56)),
        EqualTo(AttributeReference("c", StringType)(), Literal("A row")))

      val multi_column12 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        EqualTo(AttributeReference("b", IntegerType)(), Literal(56)),
        Or(GreaterThan(AttributeReference("a", IntegerType)(), Literal(15)),
          EqualTo(AttributeReference("a", IntegerType)(), Literal(18))))

      val multi_column13 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        EqualTo(AttributeReference("b", IntegerType)(), Literal(56)),
        Or(GreaterThan(AttributeReference("c", StringType)(), Literal("a row")),
          EqualTo(AttributeReference("c", StringType)(), Literal("a row"))))

      // a contradictory SQL condition
      val multi_column14 = Seq(EqualTo(AttributeReference("a", IntegerType)(), Literal(1)),
        EqualTo(AttributeReference("b", IntegerType)(), Literal(56)),
        EqualTo(AttributeReference("c", StringType)(), Literal("A row")),
        GreaterThan(AttributeReference("a", IntegerType)(), Literal(10)))

      // No requirement
      assert(!isNotNull.exists(meta.isSupportedByIndex(_, None)))
      assert(eq.exists(meta.isSupportedByIndex(_, None)))
      assert(eq3.exists(meta.isSupportedByIndex(_, None)))
      assert(lt.exists(meta.isSupportedByIndex(_, None)))
      assert(gt.exists(meta.isSupportedByIndex(_, None)))
      assert(gt2.exists(meta.isSupportedByIndex(_, None)))
      assert(lte.exists(meta.isSupportedByIndex(_, None)))
      assert(gte.exists(meta.isSupportedByIndex(_, None)))
      assert(gte1.exists(meta.isSupportedByIndex(_, None)))
      assert(inSet.exists(meta.isSupportedByIndex(_, None)))
      assert(!eq2.exists(meta.isSupportedByIndex(_, None)))
      assert(or1.exists(meta.isSupportedByIndex(_, None)))
      assert(or2.exists(meta.isSupportedByIndex(_, None)))
      assert(or3.exists(meta.isSupportedByIndex(_, None)))
      assert(!or4.exists(meta.isSupportedByIndex(_, None)))
      assert(complicated_Or.exists(meta.isSupportedByIndex(_, None)))
      assert(
        moreComplicated_Or.exists(meta.isSupportedByIndex(_, None))
      )
      assert(multi_column1.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column2.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column3.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column4.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column5.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column6.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column7.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column8.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column9.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column10.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column11.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column12.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column13.exists(meta.isSupportedByIndex(_, None)))
      assert(multi_column14.exists(meta.isSupportedByIndex(_, None)))

      // check requirements.
      val indexRequirement: IndexType = BTreeIndex()
      // btree metrics, bitmap index should fail
      assert(eq.exists(meta.isSupportedByIndex(_, Some(indexRequirement))))
      assert(!gt2.exists(meta.isSupportedByIndex(_, Some(indexRequirement))))

      val multiRequirements: Seq[IndexType] = Seq(BTreeIndex(), BitMapIndex())
      assert(
        multi_column6.zip(multiRequirements)
          .map(x => meta.isSupportedByIndex(x._1, Some(x._2))).reduce(_ && _))
    }
  }

  test("OAP should create .oap.meta_bk file in data file dir #1030 ") {
    var testSchema = new StructType()
    for(a <- 0 to 120) {
      testSchema = testSchema.add(s"test_schema_column_$a", IntegerType)
    }

    val partitions = new Array[Path](100)
    for(p <- 0 to 99) {
      val file = new File(tmpDir.getAbsolutePath, s"partition=$p")
      file.mkdirs()
      val path = new Path(
        new File(file.getAbsolutePath, "testOap.meta").getAbsolutePath)
      partitions(p) = path

    }
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(10))
    val parPartition = partitions.toSeq.par
    parPartition.tasksupport = taskSupport

    // If we revert this patch, this UT will throw one of following exceptions:
    // 1.java.io.FileNotFoundException: File testOap.meta_bk does not exist
    // 2.java.io.IOException: Could not create testOap.meta_bk
    // and this UT will fail
    try {
      parPartition.map(writeMetaFile(_))
    } catch {
      case e: FileNotFoundException => fail(e.getMessage)
      case e: IOException => fail(e.getMessage)
    }
  }

  test("test schema size larger than 65535") {
    val fields = new Array[StructField](65536)
    for (a <- 0 to 65535) {
      fields(a) = new StructField(s"large_schema_column_$a", IntegerType)
    }
    val largeSchema = StructType(fields)
    assert(largeSchema.json.getBytes(StandardCharsets.UTF_8).length > 65535)
    val path = new Path(new File(tmpDir.getAbsolutePath, "testOap.meta").getAbsolutePath)
    writeMetaFile(path, largeSchema, Version.latestVersion())
    val oapMeta = DataSourceMeta.initialize(path, new Configuration())
    assert(oapMeta.schema == largeSchema)
  }

  test("test meta version compatibility") {
    var testSchema = new StructType()
    for (a <- 0 to 10) {
      testSchema = testSchema.add(s"test_schema_column_$a", IntegerType)
    }
    for (version <- Version.allVersions()) {
      val path = new Path(new File(tmpDir.getAbsolutePath, s"test_$version.meta").getAbsolutePath)
      writeMetaFile(path, testSchema, version)
      val oapMeta = DataSourceMeta.initialize(path, new Configuration())
      assert(oapMeta.schema == testSchema)
    }
  }
}
