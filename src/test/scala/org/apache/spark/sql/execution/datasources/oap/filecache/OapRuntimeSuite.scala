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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.test.oap.SharedOapLocalClusterContext
import org.apache.spark.util.Utils

class OapRuntimeSuite extends QueryTest with BeforeAndAfterEach
  with SharedOapLocalClusterContext with SQLTestUtils {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    val path1 = Utils.createTempDir().getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW oap_sort_opt_table (a INT, b STRING)
            | USING oap
            | OPTIONS (path '$path1')""".stripMargin)
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }

  test("OapRuntime Test") {
    spark.conf.set(OapFileFormat.ROW_GROUP_SIZE, 50)

    val data = (1 to 300).map { i => (i%102, s"this is test $i") }
    val dataRDD = spark.sparkContext.parallelize(data, 4)

    dataRDD.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_sort_opt_table select * from t")

    try {
      sql("create oindex index1 on oap_sort_opt_table (a)")

      sql("SELECT * FROM oap_sort_opt_table")

    } finally {
      sql("drop oindex index1 on oap_sort_opt_table")
    }
  }
}
