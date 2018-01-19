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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object CreateDatabase {

  def main(args: Array[String]) {
    if (args.length < 4) {
      sys.error("Please config the arguments for testing!")
    }
    // e.g., 2 for 0.2.0
    val versionNum = args(0)

    // "oap" or "parquet" or "both"
    val dataFormats: Seq[String] = args(1) match {
      case "both" => Seq("oap", "parquet")
      case "oap" | "parquet" => Seq(args(1))
      case _ => Seq()
    }

    // data scale normally varies among [1, 1000]
    val dataScale = args(2)

    val testTrie = args(3)

    val conf = new Configuration()
    val hadoopFs = FileSystem.get(conf)
    val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0")
      .enableHiveSupport().getOrCreate()
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
    val sqlContext = spark.sqlContext
    val tablePath = s"hdfs:///user/oap/oaptest/oap-0.${versionNum}.0/tpcds/tpcds$dataScale/"
    spark.sql(s"create database if not exists oaptpcds$dataScale")
    spark.sql(s"create database if not exists parquettpcds$dataScale")

    def genData(dataFormat: String) = {
      val dataLocation = s"${tablePath}${dataFormat}/"
      spark.sql(s"use ${dataFormat}tpcds${dataScale}")
      spark.sql("drop table if exists customer")
      spark.sql("drop table if exists store_sales")
      if(testTrie == "true" && hadoopFs.exists(new Path(dataLocation + "customer"))) {
        val df = spark.read.format(dataFormat).load(dataLocation + "customer")
          .filter("c_email_address is not null")
        df.write.format(dataFormat).mode(SaveMode.Overwrite).save(dataLocation + "customer1")
        hadoopFs.delete(new Path(dataLocation + "customer/"), true)
        FileUtil.copy(hadoopFs, new Path(dataLocation + "customer1"),
          hadoopFs, new Path(dataLocation + "customer"), true, conf)
        sqlContext.createExternalTable("customer", dataLocation + "customer", dataFormat)
      }
      var df = spark.read.format(dataFormat).load(dataLocation + "store_sales")
      val divRatio = df.select("ss_item_sk").orderBy(desc("ss_item_sk")).limit(1).
        collect()(0)(0).asInstanceOf[Int] / 1000
      val divideUdf = udf((s: Int) => s / divRatio)
      df.withColumn("ss_item_sk1", divideUdf(col("ss_item_sk"))).write.format(dataFormat)
        .mode(SaveMode.Overwrite).save(dataLocation + "store_sales1")
      hadoopFs.delete(new Path(dataLocation + "store_sales"), true)
      FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
        hadoopFs, new Path(dataLocation + "store_sales"), true, conf)
      sqlContext.createExternalTable("store_sales", dataLocation + "store_sales", dataFormat)
      println("File size of orignial table store_sales in oap format: " +
        TestUtil.calculateFileSize("store_sales", dataLocation, dataFormat)
      )
      println("Records of table store_sales: " +
        spark.read.format(dataFormat).load(dataLocation + "store_sales").count()
      )
    }

    dataFormats.foreach(genData)
    spark.stop()
  }
}

