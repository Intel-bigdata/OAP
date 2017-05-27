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

import java.util.{Locale, TimeZone}

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.internal.SQLConf

class SpinachQuerySuite extends HiveComparisonTest with BeforeAndAfter  {
  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault
  import org.apache.spark.sql.hive.test.TestHive._

  private val originalCrossJoinEnabled = TestHive.conf.crossJoinEnabled

  override def beforeAll() {
    super.beforeAll()
    TestHive.setCacheTables(true)
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)
    // Ensures that cross joins are enabled so that we can test them
    TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, true)
    TestHive.setConf(HiveUtils.CONVERT_METASTORE_PARQUET, true)
  }

  override def afterAll() {
    try {
      TestHive.setCacheTables(false)
      TimeZone.setDefault(originalTimeZone)
      Locale.setDefault(originalLocale)
      sql("DROP TEMPORARY FUNCTION IF EXISTS udtf_count2")
      TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, originalCrossJoinEnabled)
    } finally {
      super.afterAll()
    }
  }
  private def assertDupIndex(body: => Unit): Unit = {
    val e = intercept[AnalysisException] { body }
    assert(e.getMessage.toLowerCase.contains("exists"))
  }

  test("create hive table in parquet format") {
    sql("create table p_table (key int, val string) stored as parquet")
    sql("insert overwrite table p_table select * from src")
    sql("create sindex if not exists p_index on p_table(key)")

    assert(sql("select val from p_table where key = 238").collect().head.getString(0) == "val_238")
    sql("drop table p_table")
  }

  test("create duplicate hive table in parquet format") {
    sql("create table p_table1 (key int, val string) stored as parquet")
    sql("insert overwrite table p_table1 select * from src")

    sql("create sindex p_index on p_table1(key)")
    assertDupIndex { sql("create sindex p_index on p_table1(key)") }
  }
}
