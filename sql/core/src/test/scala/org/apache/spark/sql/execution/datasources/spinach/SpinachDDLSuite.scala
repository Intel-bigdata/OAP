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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils


class SpinachDDLSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    System.setProperty("spinach.rowgroup.size", "1024")
    val path1 = Utils.createTempDir().getAbsolutePath
    val path2 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY TABLE spinach_test_1 (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path1')""".stripMargin)
    sql(s"""CREATE TEMPORARY TABLE spinach_test_2 (a INT, b STRING)
           | USING spn
           | OPTIONS (path '$path2')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("spinach_test_1")
    sqlContext.dropTempTable("spinach_test_2")
  }

  test("show index") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").registerTempTable("t")
    checkAnswer(sql("show sindex from spinach_test_1"), Nil)
    sql("insert overwrite table spinach_test_1 select * from t")
    sql("insert overwrite table spinach_test_2 select * from t")
    sql("create sindex index1 on spinach_test_1 (a)")
    checkAnswer(sql("show sindex from spinach_test_2"), Nil)
    sql("create sindex index2 on spinach_test_1 (b desc)")
    sql("create sindex index3 on spinach_test_1 (b asc, a desc)")
    sql("create sindex index4 on spinach_test_2 (a) using btree")
    sql("create sindex index5 on spinach_test_2 (b desc)")
    sql("create sindex index6 on spinach_test_2 (a) using bitmap")
    sql("create sindex index1 on spinach_test_2 (a desc, b desc)")

    checkAnswer(sql("show sindex from spinach_test_1"),
      Row("spinach_test_1", "index1", 0, "a", "A", "BTREE") ::
        Row("spinach_test_1", "index2", 0, "b", "D", "BTREE") ::
        Row("spinach_test_1", "index3", 0, "b", "A", "BTREE") ::
        Row("spinach_test_1", "index3", 1, "a", "D", "BTREE") :: Nil)

    checkAnswer(sql("show sindex in spinach_test_2"),
      Row("spinach_test_2", "index4", 0, "a", "A", "BTREE") ::
        Row("spinach_test_2", "index5", 0, "b", "D", "BTREE") ::
        Row("spinach_test_2", "index6", 0, "a", "A", "BITMAP") ::
        Row("spinach_test_2", "index1", 0, "a", "D", "BTREE") ::
        Row("spinach_test_2", "index1", 1, "b", "D", "BTREE") :: Nil)
  }
}

