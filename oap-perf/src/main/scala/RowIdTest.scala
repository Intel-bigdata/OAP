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

package org.apache.spark.sql.oap.perf

import com.databricks.spark.sql.perf.tpcds.Tables

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * Used for testing on very large data set in just one partition.
 * For example, if oap perfroms correctly for one partition with 2g rows and
 * should throw exception for more than 2g rows partition.
 * This will be extended to more pressure tests later.
 */
object RowIdTest {

  def main(args: Array[String]) {
    if (args.length < 1) {
      sys.error("Please config the arguments for testing!")
    }
    val versionNum = args(0) // e.g., 2 for 0.2.0
    val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0").enableHiveSupport().getOrCreate()
    val sqlContext = spark.sqlContext
    val tables = new Tables(spark.sqlContext, "/home/oap/tpcds-kit/tools", 1000)
    val oapDataLocation = s"oaptest/oap-0.${versionNum}.0/tpcds/tpcds1000/oap/"

    tables.genData(oapDataLocation, "oap", true, false, true, false, false, "store_sales", 4)
    spark.sql("create database if not exists oaptpcds1000")
    spark.sql("use oaptpcds1000")
    spark.sql("drop table if exists largetable")
    spark.sql("drop table if exists normaltable")

    val df = spark.read.format("oap").load(oapDataLocation + "store_sales")
    val divRatio = df.select("ss_ticket_number").orderBy(desc("ss_ticket_number"))
      .limit(1).collect()(0)(0).asInstanceOf[Int] / 100
    df.select(col("ss_ticket_number").divide(divRatio).as("ss_ticket_number").cast(IntegerType))
      .write.format("oap").mode(SaveMode.Overwrite).save(oapDataLocation + "largetable")
    df.select(col("ss_ticket_number").divide(divRatio).as("ss_ticket_number").cast(IntegerType))
      .limit(Int.MaxValue).write.format("oap")
      .mode(SaveMode.Overwrite).save(oapDataLocation + "normaltable")
    spark.sql("drop table if exists largetable")
    spark.sql("drop table if exists normaltable")
    sqlContext.createExternalTable("largetable", oapDataLocation + "largetable", "oap")
    sqlContext.createExternalTable("normaltable", oapDataLocation + "normaltable", "oap")
    try {
      spark.sql("DROP OINDEX normalindex1 ON normaltable")
    } catch {
      case _ =>
    }
    spark.sql(
      "CREATE OINDEX IF NOT EXISTS normalindex ON normaltable (ss_ticket_number) USING BTREE"
    )
    spark.stop()
  }
}

