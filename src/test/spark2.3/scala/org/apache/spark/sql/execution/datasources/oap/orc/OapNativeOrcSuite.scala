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

package org.apache.spark.sql.execution.datasources.oap.orc

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex, TestPartition}
import org.apache.spark.util.Utils

class OapNativeOrcSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    val path1 = Utils.createTempDir().getAbsolutePath
    val path2 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW native_orc_test (a INT, b STRING)
           | USING org.apache.spark.sql.execution.datasources.orc
           | OPTIONS (path '$path1')""".stripMargin)
    sql(s"""CREATE TEMPORARY VIEW orc_test (a INT, b STRING)
           | USING orc
           | OPTIONS (path '$path2')""".stripMargin)
    sql(s"""CREATE TABLE native_orc_partition_table (a int, b int, c STRING)
            | USING org.apache.spark.sql.execution.datasources.orc
            | PARTITIONED by (b, c)""".stripMargin)
    sql(s"""CREATE TEMPORARY VIEW orc_table_2 (a INT, b INT)
           | USING orc
           | OPTIONS (path '$path2')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("native_orc_test")
    sqlContext.dropTempTable("orc_test")
    sqlContext.sql("drop table native_orc_partition_table")
    sqlContext.sql("drop table orc_table_2")
  }

  test("test native orc functionality with and without index ") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table native_orc_test select * from t")
    checkAnswer(sql("select * from native_orc_test where a < 3"),
      Row(1, "this is test 1") :: Row(2, "this is test 2") :: Nil)
    // Test btree index.
    sql("create oindex idx on native_orc_test (a)")
    checkAnswer(sql("select * from native_orc_test where a < 3"),
      Row(1, "this is test 1") :: Row(2, "this is test 2") :: Nil)
    sql("drop oindex idx on native_orc_test")

    // Test bitmap index.
    sql("create oindex idx on native_orc_test (a) using BITMAP")
    checkAnswer(sql("select * from native_orc_test where a == 4"),
      Row(4, "this is test 4") :: Nil)
    sql("drop oindex idx on native_orc_test")
  }

  test("test writing for hive orc and reading for native orc with and without index ") {
    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table orc_test select * from t")
      checkAnswer(sql("select * from orc_test where a < 3"),
        Row(1, "this is test 1") :: Row(2, "this is test 2") :: Nil)

      sql("create oindex idx on orc_test (a)")
      checkAnswer(sql("select * from orc_test where a < 3"),
        Row(1, "this is test 1") :: Row(2, "this is test 2") :: Nil)
      sql("drop oindex idx on orc_test")
    }
  }

  test("create and drop index with partition specify with native orc") {
    val data: Seq[(Int, Int)] = (1 to 10).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")

    val path = new Path(sqlContext.conf.warehousePath)

    sql(
      """
        |INSERT OVERWRITE TABLE native_orc_partition_table
        |partition (b=1, c='c1')
        |SELECT key from t where value < 4
      """.stripMargin)

    sql(
      """
        |INSERT INTO TABLE native_orc_partition_table
        |partition (b=2, c='c2')
        |SELECT key from t where value == 4
      """.stripMargin)

    withIndex(
      TestIndex("native_orc_partition_table", "index1",
        TestPartition("b", "1"), TestPartition("c", "c1"))) {
      sql("create oindex index1 on native_orc_partition_table (a) partition (b=1, c='c1')")

      checkAnswer(sql("select * from native_orc_partition_table where a < 4"),
        Row(1, 1, "c1") :: Row(2, 1, "c1") :: Row(3, 1, "c1") :: Nil)

      assert(path.getFileSystem(
        configuration).globStatus(new Path(path,
        "native_orc_partition_table/b=1/c=c1/*.index")).length != 0)
      assert(path.getFileSystem(
        configuration).globStatus(new Path(path,
        "native_orc_partition_table/b=2/c=c2/*.index")).length == 0)
    }

    withIndex(
      TestIndex("native_orc_partition_table", "index1",
        TestPartition("b", "2"), TestPartition("c", "c2"))) {
      sql("create oindex index1 on native_orc_partition_table (a) partition (b=2, c='c2')")

      checkAnswer(sql("select * from native_orc_partition_table"),
        Row(1, 1, "c1") :: Row(2, 1, "c1") :: Row(3, 1, "c1") :: Row(4, 2, "c2") :: Nil)
      assert(path.getFileSystem(
        configuration).globStatus(new Path(path,
        "native_orc_partition_table/b=1/c=c1/*.index")).length == 0)
      assert(path.getFileSystem(
        configuration).globStatus(new Path(path,
        "native_orc_partition_table/b=2/c=c2/*.index")).length != 0)
    }
  }

  test("create duplicated name index with native orc") {
    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      val data: Seq[(Int, String)] = (1 to 100).map { i => (i, s"this is test $i") }
      val df = data.toDF("a", "b")
      val pathDir = Utils.createTempDir().getAbsolutePath
      df.write.format("orc").mode(SaveMode.Overwrite).save(pathDir)
      val oapDf = spark.read.format("orc").load(pathDir)
      oapDf.createOrReplaceTempView("t")

      withIndex(TestIndex("t", "idxa")) {
        sql("create oindex idxa on t (a)")
        val path = new Path(pathDir)
        val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
        val indexFiles1 = fs.listStatus(path).collect { case fileStatus if fileStatus.isFile &&
          fileStatus.getPath.getName.endsWith(OapFileFormat.OAP_INDEX_EXTENSION) =>
          fileStatus.getPath.getName
        }

        sql("create oindex if not exists idxa on t (a)")
        val indexFiles2 = fs.listStatus(path).collect { case fileStatus if fileStatus.isFile &&
          fileStatus.getPath.getName.endsWith(OapFileFormat.OAP_INDEX_EXTENSION) =>
          fileStatus.getPath.getName
        }
        assert(indexFiles1 === indexFiles2)
      }
    }
  }
}
