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
package org.apache.spark.sql.suites

import org.apache.spark.sql._
import org.apache.spark.util.Utils

object LocalSparkMasterTestSuite extends OapTestSuite with LocalClusterConfigSet {

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }
  import testImplicits._

  override def isBootStrapping: Boolean = true

  override protected def getAppName: String = "LocalSparkMasterTestSuite"

  def isDataExists(): Boolean = {
    val db = spark.sqlContext.sql(s"show databases").collect()
    if (db.exists(_.getString(0) == databaseName)) {
      spark.sqlContext.sql(s"USE $databaseName")
      val table = spark.sqlContext.sql(s"show tables").collect()
      if (table.exists(_.getString(1) == tableName)) true
      else false
    } else {
      false
    }
  }

  def prepareData(): Boolean = {
    val conf = activeConf
    val path = Utils.createTempDir().getAbsolutePath
    val format = conf.getBenchmarkConf(BenchmarkConfig.FILE_FORMAT)
    val data: Seq[(Int, Int)] = (1 to 3000).map { i => (i, i)}
    data.toDF("rowId", attr).write.mode("overwrite").format(format).save(path)
    spark.sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    spark.sqlContext.createExternalTable(s"$databaseName.$tableName", path, format)

    // TODO: Drop table and index after all.
    if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE).toBoolean) {
      spark.sql(s"create oindex ${attr}_index on $databaseName.$tableName ($attr)")
    }

    spark.sqlContext.sql(s"USE $databaseName")
    true
  }

  def databaseName = {
    val conf = activeConf
    conf.getBenchmarkConf(BenchmarkConfig.FILE_FORMAT) match {
      case "parquet" => "parquet_base"
      case "oap" => "oap_target"
      case _ => "default"
    }
  }

  val tableName = "test_table"

  val attr = "test_column"

  override def prepare(): Boolean = {
    if (isDataExists()) {
      true
    } else {
      prepareData()
    }
  }

  override def testSet = Seq(
    OapBenchmarkTest("eq = 1",
      s"SELECT * FROM $tableName WHERE $attr = 1"),
    OapBenchmarkTest("eq = 2",
      s"SELECT * FROM $tableName WHERE $attr = 2"),
    OapBenchmarkTest("check how many char can be displayed in name column-54--------64-----",
      s"SELECT * FROM $tableName WHERE $attr = 2")

  )
}
