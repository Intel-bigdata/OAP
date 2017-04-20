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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils


class EncodeCompressionSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    System.setProperty("spinach.rowgroup.size", "1024")
    System.setProperty("spinach.compression.codec", "GZIP")
    System.setProperty("spinach.encoding.dictionaryEnabled", "true")
    val path = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY TABLE spinach_test (a INT, b STRING, c DOUBLE, d BOOLEAN)
           | USING spn
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("spinach_test")
  }
  test("filtering by string") {
    val data: Seq[(Int, String, Double, Boolean)] = (1 to 300).map { i =>
      (i, s"this is test $i", i.toDouble, i % 13 == 0)
    }
    data.toDF("int", "string", "double", "boolean").registerTempTable("t")
    sql("insert overwrite table spinach_test select * from t")

    checkAnswer(sql("SELECT * FROM spinach_test"), data.toDF())

  }
}
