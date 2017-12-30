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

package org.apache.spark.sql.execution.datasources

import sys.process._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.oap.ClusterTestContext
import org.apache.spark.util.Utils

class ClusterTestSuite extends QueryTest with ClusterTestContext {

  import testImplicits._

  private val wgetRetryTimes = 25
  private var currentPath: String = _

  private val workDir = ("pwd" !!).replace("\n", "") + "/spark"
  private val sparkVersion = "spark-2.1.0-bin-hadoop2.7"
  private val sparkHome = s"${workDir}/${sparkVersion}"

  /**
   * Prepare Spark in Travis Platform and other work.
   */
  override def beforeAll(): Unit = {
    // prepare spark and start it in standalone mode
    assert(("test -e spark" #|| "mkdir spark" !) == 0)
    assert((s"wget -q -t ${wgetRetryTimes} -P ${workDir}" +
      s" https://archive.apache.org/dist/spark/spark-2.1.0/${sparkVersion}.tgz" !) == 0)
    assert((s"tar -xf ${workDir}/${sparkVersion}.tgz -C ${workDir}" !) == 0)
    assert((("echo \"spark.yarn.jars=" + sparkHome + "/jars/*.jar\" > " +
      sparkHome + "/conf/spark-defaults.conf") !) == 0)
    assert(("echo \"localhost\" > " + s"${sparkHome}/conf/slaves" !) == 0)
    assert((s"${sparkHome}/sbin/start-master.sh --ip localhost --port 7077" #&&
      s"${sparkHome}/sbin/start-slave.sh spark://localhost:7077" !) == 0)
    super.beforeAll()
  }

  /**
   * Clean related directory.
   */
  protected override def afterAll(): Unit = {
    super.afterAll()
    assert((s"${sparkHome}/sbin/stop-master.sh" #&&
      s"${sparkHome}/sbin/stop-slave.sh" !) == 0)
    assert((s"rm -rf ${workDir}/../spark" !) == 0)
  }

  override def beforeEach(): Unit = {
    spark.conf.set(SQLConf.OAP_INDEXER_CHOICE_MAX_SIZE.key, "2")
    spark.conf.set(SQLConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key, "true")
    spark.conf.set(SQLConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.key, "1000")
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path
    sql(
      s"""CREATE TEMPORARY VIEW oap_test (a INT, b INT, c INT)
         | USING oap
         | OPTIONS (path '$path')""".stripMargin)

    sql(
      s"""CREATE TEMPORARY VIEW parquet_test (a INT, b INT, c INT)
         | USING parquet
         | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
    sqlContext.dropTempTable("parquet_test")
    spark.conf.unset(SQLConf.OAP_INDEXER_CHOICE_MAX_SIZE.key)
    spark.conf.unset(SQLConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key)
    spark.conf.unset(SQLConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.key)
  }

  test("filtering parquet in cluster mode") {
    val data: Seq[(Int, Int, Int)] = (1 to 200).map { i => (i % 13, (300 - i) % 17, i) }
    data.toDF("a", "b", "c").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    sql("create oindex index1 on parquet_test (a)")
    sql("create oindex index2 on parquet_test (b) USING BITMAP")

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a = 1 and b = 10"),
      Row(1) :: Nil)

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a > 3 and a < 5 and b = 14"),
      Row(82) :: Nil)

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a > 3 and a < 5 and b = 14 and c > 30"),
      Row(82) :: Nil)

    sql("drop oindex index1 on parquet_test")
    sql("drop oindex index2 on parquet_test")

    sql("create oindex index1 on parquet_test (a,b)")
    sql("create oindex index2 on parquet_test (c)")

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a = 3 and b > 14 and c > 30"),
      Row(81) :: Nil)

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a = 3 and b > 14 and c > 300"),
      Nil)

    sql("drop oindex index1 on parquet_test")
    sql("drop oindex index2 on parquet_test")
  }

  test("filtering oap in cluster mode") {
    val data: Seq[(Int, Int, Int)] = (1 to 200).map { i => (i % 13, (300 - i) % 17, i) }
    data.toDF("a", "b", "c").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    sql("create oindex index1 on oap_test (a)")
    sql("create oindex index2 on oap_test (b)  USING BITMAP")

    checkAnswer(sql("SELECT c FROM oap_test WHERE a = 1 and b = 10"),
      Row(1) :: Nil)

    checkAnswer(sql("SELECT c FROM oap_test WHERE a > 3 and a < 5 and b = 14"),
      Row(82) :: Nil)

    checkAnswer(sql("SELECT c FROM oap_test WHERE a > 3 and a < 5 and b = 14 and c > 30"),
      Row(82) :: Nil)

    sql("drop oindex index1 on oap_test")
    sql("drop oindex index2 on oap_test ")


    sql("create oindex index1 on oap_test (a,b)")
    sql("create oindex index2 on oap_test (c)")

    checkAnswer(sql("SELECT c FROM oap_test WHERE a = 3 and b > 14 and c > 30"),
      Row(81) :: Nil)

    checkAnswer(sql("SELECT c FROM oap_test WHERE a = 3 and b > 14 and c > 300"), Nil)

    sql("drop oindex index1 on oap_test")
    sql("drop oindex index2 on oap_test")
  }
}
