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

import org.apache.spark.sql.SparkSession

/**
 * Gen data using TPCDS dataset. We use store_sales table for B-Tree and Bitmap index,
 * and customer table for Trie index.
 */
object DataGen {
  def main(args: Array[String]) {
    if (args.length < 3) {
      sys.error("Please config the version of OAP to test!")
    }
    else {
      val versionNum = args(0)
      val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0")
        .enableHiveSupport().getOrCreate()
      spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
      val scale = args(1).toInt
      val testTrie = args(2)
      val partitions = args(3).toInt
      val tables1 = new Tables(spark.sqlContext, "/home/oap/tpcds-kit/tools", scale)
      val dataLocationOap = s"oaptest/oap-0.${versionNum}.0/tpcds/tpcds$scale/oap/"
      val dataLocationParquet = s"oaptest/oap-0.${versionNum}.0/tpcds/tpcds$scale/parquet/"
      tables1.genData(dataLocationOap, "oap", true, false, true, false, false,
        "store_sales", partitions)
      tables1.genData(dataLocationParquet, "parquet", true, false, true, false, false,
        "store_sales", partitions)
      if(testTrie == "true") {
        val tables2 = new Tables(spark.sqlContext, "/home/oap/tpcds-kit/tools", 1000)
        tables2.genData(dataLocationOap, "oap", true, false, true, false, false,
          "customer", partitions)
        tables2.genData(dataLocationParquet, "parquet", true, false, true, false, false,
          "customer", partitions)
      }
      spark.stop()
    }
  }
}
