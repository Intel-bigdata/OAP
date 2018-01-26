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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object CreateDatabase {

  def main(args: Array[String]) {
    if (args.length < 5) {
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
    val hdfsPath = args(4)

    val conf = new Configuration()
    val hadoopFs = FileSystem.get(conf)
    val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0")
      .enableHiveSupport().getOrCreate()
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
    val sqlContext = spark.sqlContext
    val tablePath = s"${hdfsPath}oap-0.${versionNum}.0/tpcds/tpcds$dataScale/"
    spark.sql(s"create database if not exists oap_tpcds_$dataScale")
    spark.sql(s"create database if not exists parquet_tpcds_$dataScale")

    def genData(dataFormat: String) = {
      val dataLocation = s"${tablePath}${dataFormat}/"
      spark.sql(s"use ${dataFormat}_tpcds_${dataScale}")
      spark.sql("drop table if exists customer")
      spark.sql("drop table if exists store_sales")
      spark.sql("drop table if exists store_sales_dup")
      if(testTrie == "true" && hadoopFs.exists(new Path(dataLocation + "customer"))) {
        val df = spark.read.format(dataFormat).load(dataLocation + "customer")
          .filter("c_email_address is not null")
        df.write.format(dataFormat).mode(SaveMode.Overwrite).save(dataLocation + "customer1")
        hadoopFs.delete(new Path(dataLocation + "customer/"), true)
        FileUtil.copy(hadoopFs, new Path(dataLocation + "customer1"),
          hadoopFs, new Path(dataLocation + "customer"), true, conf)
        sqlContext.createExternalTable("customer", dataLocation + "customer", dataFormat)
      }

      /**
       * To compare performance between B-Tree and Bitmap index, we generate duplicate
       * tables of store_sales here. Besides, store_sales_dup table can be used in testing
       * OAP strategies.
       */
      var df = spark.read.format(dataFormat).load(dataLocation + "store_sales")
      val divRatio = df.select("ss_item_sk").orderBy(desc("ss_item_sk")).limit(1).
        collect()(0)(0).asInstanceOf[Int] / 1000
      val divideUdf = udf((s: Int) => s / divRatio)
      df.withColumn("ss_item_sk1", divideUdf(col("ss_item_sk"))).write.format(dataFormat)
        .mode(SaveMode.Overwrite).save(dataLocation + "store_sales1")
      hadoopFs.delete(new Path(dataLocation + "store_sales"), true)

      // Notice here delete source flag should firstly be set to false
      FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
        hadoopFs, new Path(dataLocation + "store_sales"), false, conf)
      FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
        hadoopFs, new Path(dataLocation + "store_sales_dup"), true, conf)
      sqlContext.createExternalTable("store_sales", dataLocation + "store_sales", dataFormat)
      sqlContext.createExternalTable("store_sales_dup", dataLocation + "store_sales_dup", dataFormat)
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

