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

package org.apache.spark.sql.hive.execution

import java.io.File

import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterEach, Ignore}
import org.apache.spark.sql.{QueryTest, Row, SparkSession, TestOap}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.datasources.oap.utils.OapUtils
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils


// test OAP Index DDL&DML on Hive tables. Ignored as cases were moved to OapPlannerSuite.
// In future we can re-use this one if we offer individual warehouse dir for different suite.
@Ignore
class HiveOapIndexDDLSuite
  extends QueryTest with SQLTestUtils with BeforeAndAfterEach {
  import testImplicits._

  override def spark: SparkSession = TestHive.sparkSession

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
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
