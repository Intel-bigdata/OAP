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

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class OapDDLSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    val path1 = Utils.createTempDir().getAbsolutePath
    val path2 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW oap_test_1 (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path1')""".stripMargin)
    sql(s"""CREATE TEMPORARY VIEW oap_test_2 (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path2')""".stripMargin)
    sql(s"""CREATE TABLE oap_partition_table (a int, b int, c STRING)
            | USING parquet
            | PARTITIONED by (b, c)""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test_1")
    sqlContext.dropTempTable("oap_test_2")
    sqlContext.sql("drop table oap_partition_table")
  }

  test("write index for table read in from DS api") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    val df = data.toDF("key", "value")
    // TODO test when path starts with "hdfs:/"
    val path = Utils.createTempDir("/tmp/").toString
    df.write.format("oap").mode(SaveMode.Overwrite).save(path)
    val oapDf = spark.read.format("oap").load(path)
    oapDf.createOrReplaceTempView("t")
    sql("create oindex index1 on t (key)")
  }

  test("show index") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    checkAnswer(sql("show oindex from oap_test_1"), Nil)
    sql("insert overwrite table oap_test_1 select * from t")
    sql("insert overwrite table oap_test_2 select * from t")
    sql("create oindex index1 on oap_test_1 (a)")
    checkAnswer(sql("show oindex from oap_test_2"), Nil)
    sql("create oindex index2 on oap_test_1 (b desc)")
    sql("create oindex index3 on oap_test_1 (b asc, a desc)")
    sql("create oindex index4 on oap_test_2 (a) using btree")
    sql("create oindex index5 on oap_test_2 (b desc)")
    sql("create oindex index6 on oap_test_2 (a) using bitmap")
    sql("create oindex index1 on oap_test_2 (a desc, b desc)")

    checkAnswer(sql("show oindex from oap_test_1"),
      Row("oap_test_1", "index1", 0, "a", "A", "BTREE", true) ::
        Row("oap_test_1", "index2", 0, "b", "D", "BTREE", true) ::
        Row("oap_test_1", "index3", 0, "b", "A", "BTREE", true) ::
        Row("oap_test_1", "index3", 1, "a", "D", "BTREE", true) :: Nil)

    checkAnswer(sql("show oindex in oap_test_2"),
      Row("oap_test_2", "index4", 0, "a", "A", "BTREE", true) ::
        Row("oap_test_2", "index5", 0, "b", "D", "BTREE", true) ::
        Row("oap_test_2", "index6", 0, "a", "A", "BITMAP", true) ::
        Row("oap_test_2", "index1", 0, "a", "D", "BTREE", true) ::
        Row("oap_test_2", "index1", 1, "b", "D", "BTREE", true) :: Nil)
  }

  test("create and drop index with partition specify") {
    val data: Seq[(Int, Int)] = (1 to 10).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")

    val path = new Path(sqlConf.warehousePath)

    sql(
      """
        |INSERT OVERWRITE TABLE oap_partition_table
        |partition (b=1, c='c1')
        |SELECT key from t where value < 4
      """.stripMargin)

    sql(
      """
        |INSERT INTO TABLE oap_partition_table
        |partition (b=2, c='c2')
        |SELECT key from t where value == 4
      """.stripMargin)

    sql("create oindex index1 on oap_partition_table (a) partition (b=1, c='c1')")

    checkAnswer(sql("select * from oap_partition_table where a < 4"),
      Row(1, 1, "c1") :: Row(2, 1, "c1") :: Row(3, 1, "c1") :: Nil)

    assert(path.getFileSystem(
      configuration).globStatus(new Path(path,
        "oap_partition_table/b=1/c=c1/*.index")).length != 0)
    assert(path.getFileSystem(
      configuration).globStatus(new Path(path,
        "oap_partition_table/b=2/c=c2/*.index")).length == 0)

    sql("create oindex index1 on oap_partition_table (a) partition (b=2, c='c2')")
    sql("drop oindex index1 on oap_partition_table partition (b=1, c='c1')")

    checkAnswer(sql("select * from oap_partition_table"),
      Row(1, 1, "c1") :: Row(2, 1, "c1") :: Row(3, 1, "c1") :: Row(4, 2, "c2") :: Nil)
    assert(path.getFileSystem(
      configuration).globStatus(new Path(path,
      "oap_partition_table/b=1/c=c1/*.index")).length == 0)
    assert(path.getFileSystem(
      configuration).globStatus(new Path(path,
      "oap_partition_table/b=2/c=c2/*.index")).length != 0)
  }

  test("create duplicated name index") {
    val data: Seq[(Int, String)] = (1 to 100).map { i => (i, s"this is test $i") }
    val df = data.toDF("a", "b")
    val pathDir = Utils.createTempDir().getAbsolutePath
    df.write.format("oap").mode(SaveMode.Overwrite).save(pathDir)
    val oapDf = spark.read.format("oap").load(pathDir)
    oapDf.createOrReplaceTempView("t")
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
