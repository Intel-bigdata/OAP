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

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.datasources.oap.utils.OapUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

/**
 * OapPlannerSuite has its own spark context which initializes OapSession
 * instead of normal SparkSession. Now we have oapStrategies in planner
 * itself, so we don't need to change extraStrategies.
 *
 * Note: This function test OapSession with TestOAP context which involves
 * Hive context. As 'instantiating multiple copies of the hive metastore seems
 * to lead to weird non-deterministic failures' (see TestHive.scala), we merge
 * all Hive context related cases into this Suite to avoid multi contexts
 * conflict.
 *
 * TODO: Another way to avoid Hive error is making different warehouse Dirs for
 * each hive related cases, do it in future if hive cases grow rapidly.
 */
class OapPlannerSuite
  extends QueryTest
  with SQLTestUtils
  with BeforeAndAfterEach
{
  import testImplicits._

  // Using TestOap as oap test context.
  protected override def spark = TestOap.sparkSession

  override def beforeEach(): Unit = {
    val path1 = Utils.createTempDir().getAbsolutePath
    val path2 = Utils.createTempDir().getAbsolutePath
    val path3 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW oap_sort_opt_table (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path1')""".stripMargin)

    sql(s"""CREATE TEMPORARY VIEW oap_distinct_opt_table (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path2')""".stripMargin)

    sql(s"""CREATE TEMPORARY VIEW oap_fix_length_schema_table (a INT, b INT)
           | USING oap
           | OPTIONS (path '$path3')""".stripMargin)
  }

  override def afterEach(): Unit = {
    spark.sqlContext.dropTempTable("oap_sort_opt_table")
    spark.sqlContext.dropTempTable("oap_distinct_opt_table")
    spark.sqlContext.dropTempTable("oap_fix_length_schema_table")
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("SortPushDown Test") {
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i%102, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")
    sql("create oindex index2 on oap_sort_opt_table (b)")

    // check strategy is applied.
    checkKeywordsExist(
      sql("explain SELECT a FROM oap_sort_opt_table WHERE a >= 0 AND a <= 10 ORDER BY a LIMIT 7"),
      "OapOrderLimitFileScanExec")

    // ASC
    checkAnswer(
      sql("SELECT a FROM oap_sort_opt_table WHERE a >= 0 AND a <= 10 ORDER BY a LIMIT 7"),
                Row(0) :: Row(0) :: Row(1) :: Row(1) :: Row(1) :: Row(2) :: Row(2) :: Nil)

    // DESC
    checkAnswer(
      sql("SELECT a FROM oap_sort_opt_table WHERE a >= 90 AND a <= 101 ORDER BY a DESC LIMIT 14"),
          Row(101) :: Row(101) :: Row(100) :: Row(100) :: Row(99) :: Row(99) :: Row(98) ::
          Row( 98) :: Row( 97) :: Row( 97) :: Row( 96) :: Row(96) :: Row(96) :: Row(95) :: Nil)

    sql("drop oindex index1 on oap_sort_opt_table")
    sql("drop oindex index2 on oap_sort_opt_table")
  }

  test("SortPushDown Test with Different Project") {
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")
    sql("create oindex index2 on oap_sort_opt_table (b)")

    checkAnswer(
      sql("SELECT b FROM oap_sort_opt_table WHERE a >= 1 AND a <= 10 ORDER BY a LIMIT 4"),
        Row("this is test 1") ::
        Row("this is test 2") ::
        Row("this is test 3") ::
        Row("this is test 4") :: Nil)

    sql("drop oindex index1 on oap_sort_opt_table")
    sql("drop oindex index2 on oap_sort_opt_table")
  }

  test("Distinct index scan if SemiJoin Test") {
    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")

    val data1 = (1 to 300).map{ i => (i % 10, s"this is test $i")}
    val dataRDD1 = spark.sparkContext.parallelize(data1, 5)

    dataRDD1.toDF("key", "value").createOrReplaceTempView("t1")
    sql("insert overwrite table oap_distinct_opt_table select * from t1")
    sql("create oindex index1 on oap_distinct_opt_table (a) using bitmap")

    checkKeywordsExist(
      sql("explain SELECT * " +
      "FROM oap_sort_opt_table t1 " +
      "WHERE EXISTS " +
      "(SELECT 1 FROM oap_distinct_opt_table t2 " +
      "WHERE t1.a = t2.a AND t2.a IN (1, 2, 3, 4)) " +
      "ORDER BY a"), "OapDistinctFileScanExec")

    checkAnswer(
      sql("SELECT * " +
      "FROM oap_sort_opt_table t1 " +
      "WHERE EXISTS " +
      "(SELECT 1 FROM oap_distinct_opt_table t2 " +
      "WHERE t1.a = t2.a AND t2.a >= 1 AND t1.a < 5) " +
      "ORDER BY a"),
      Seq(
        Row(1, "this is test 1"),
        Row(2, "this is test 2"),
        Row(3, "this is test 3"),
        Row(4, "this is test 4")))

    sql("drop oindex index1 on oap_sort_opt_table")
    sql("drop oindex index1 on oap_distinct_opt_table")
  }

  test("OapFileScan WholeStageCodeGen Check") {
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i => (i, s"this is test $i")}
    val dataRDD = spark.sparkContext.parallelize(data, 10)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")
    sql("create oindex index1 on oap_sort_opt_table (a)")

    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    val sqlString =
      "explain SELECT a FROM oap_sort_opt_table WHERE a >= 0 AND a <= 10 ORDER BY a LIMIT 7"

    // OapOrderLimitFileScanExec is applied.
    checkKeywordsExist(sql(sqlString), "OapOrderLimitFileScanExec")
    // OapOrderLimitFileScanExec WholeStageCodeGen is disabled.
    checkKeywordsNotExist(sql(sqlString), "*OapOrderLimitFileScanExec")

    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    // OapOrderLimitFileScanExec WholeStageCodeGen is enabled.
    checkKeywordsExist(sql(sqlString), "*OapOrderLimitFileScanExec")

    sql("drop oindex index1 on oap_sort_opt_table")
  }

  test("aggregations with group by test") {
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)
    val data = (1 to 300).map{ i =>
      (i % 101, i % 37)
    }
    val dataRDD = spark.sparkContext.parallelize(data, 2)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_fix_length_schema_table select * from t")
    sql("create oindex index1 on oap_fix_length_schema_table (a)")

    val sqlString =
      "SELECT a, min(b), max(b), std(b), avg(b), first(b), last(b) " +
        "FROM oap_fix_length_schema_table " +
        "where a < 30 " +
        "group by a"

    checkKeywordsExist(sql("explain " + sqlString), "*OapAggregationFileScanExec")
    val oapDF = sql(sqlString).collect()

    spark.sqlContext.setConf(SQLConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key, "true")
    checkKeywordsNotExist(sql("explain " + sqlString), "OapAggregationFileScanExec")
    val baseDF = sql(sqlString)

    checkAnswer(baseDF, oapDF)
    spark.sqlContext.setConf(SQLConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key, "false")
    sql("drop oindex index1 on oap_fix_length_schema_table")
  }

  test("create oap index on external tables in default database") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)
        (1 to 300).map { i => (i, s"this is test $i") }.toDF("a", "b").createOrReplaceTempView("t")
        sql(
          s"""
             |create table $tabName
             |stored as parquet
             |location '$tmpDir'
             |as select * from t
          """.stripMargin)

        val hiveTable =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL)

        assert(tmpDir.listFiles.nonEmpty)
        checkAnswer(sql(s"create oindex idxa on $tabName(a)"), Nil)

        checkAnswer(sql(s"show oindex from $tabName"), Row(tabName, "idxa", 0, "a", "A", "BTREE"))
        sql(s"DROP TABLE $tabName")
        assert(tmpDir.listFiles.nonEmpty)
      }
    }
  }

  test("drop oap index on external tables in default database") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)
        (1 to 300).map { i => (i, s"this is test $i") }.toDF("a", "b").createOrReplaceTempView("t")
        sql(
          s"""
             |create table $tabName
             |stored as parquet
             |location '$tmpDir'
             |as select * from t
          """.stripMargin)

        val hiveTable =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL)

        assert(tmpDir.listFiles.nonEmpty)
        checkAnswer(sql(s"create oindex idxa on $tabName(a)"), Nil)

        checkAnswer(sql(s"show oindex from $tabName"), Row(tabName, "idxa", 0, "a", "A", "BTREE"))

        sql(s"drop oindex idxa on $tabName")
        checkAnswer(sql(s"show oindex from $tabName"), Nil)
        sql(s"DROP TABLE $tabName")
        assert(tmpDir.listFiles.nonEmpty)
      }
    }
  }

  test("refresh oap index on external tables in default database") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)
        (1 to 300).map { i => (i, s"this is test $i") }.toDF("a", "b").createOrReplaceTempView("t")
        sql(
          s"""
             |create table $tabName
             |stored as parquet
             |location '$tmpDir'
             |as select * from t
          """.stripMargin)

        val hiveTable =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL)

        assert(tmpDir.listFiles.nonEmpty)
        checkAnswer(sql(s"create oindex idxa on $tabName(a)"), Nil)

        checkAnswer(sql(s"show oindex from $tabName"), Row(tabName, "idxa", 0, "a", "A", "BTREE"))

        // test refresh oap index
        (500 to 600).map { i => (i, s"this is test $i") }.toDF("a", "b")
          .createOrReplaceTempView("t2")
        sql(s"insert into table $tabName select * from t2")
        sql(s"refresh oindex on $tabName")
        checkAnswer(sql(s"show oindex from $tabName"), Row(tabName, "idxa", 0, "a", "A", "BTREE"))
        checkAnswer(sql(s"select a from $tabName where a=555"), Row(555))
        sql(s"DROP TABLE $tabName")
        assert(tmpDir.listFiles.nonEmpty)
      }
    }
  }

  test("check oap index on external tables in default database") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)
        (1 to 300).map { i => (i, s"this is test $i") }.toDF("a", "b").createOrReplaceTempView("t")
        sql(
          s"""
             |create table $tabName
             |stored as parquet
             |location '$tmpDir'
             |as select * from t
          """.stripMargin)

        val hiveTable =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL)

        assert(tmpDir.listFiles.nonEmpty)
        checkAnswer(sql(s"create oindex idxa on $tabName(a)"), Nil)

        checkAnswer(sql(s"show oindex from $tabName"), Row(tabName, "idxa", 0, "a", "A", "BTREE"))

        // test check oap index
        checkAnswer(sql(s"check oindex on $tabName"), Nil)
        val dirPath = tmpDir.getAbsolutePath
        val metaOpt = OapUtils.getMeta(sparkContext.hadoopConfiguration, new Path(dirPath))
        assert(metaOpt.nonEmpty)
        assert(metaOpt.get.fileMetas.nonEmpty)
        assert(metaOpt.get.indexMetas.nonEmpty)
        val dataFileName = metaOpt.get.fileMetas.head.dataFileName
        // delete a data file
        Utils.deleteRecursively(new File(dirPath, dataFileName))

        // Check again
        checkAnswer(
          sql(s"check oindex on $tabName"),
          Row(s"Data file: $dirPath/$dataFileName not found!"))

        sql(s"DROP TABLE $tabName")
        assert(tmpDir.listFiles.nonEmpty)
      }
    }
  }
}
