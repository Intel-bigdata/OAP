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
package org.apache.spark.sql

import java.io.FileNotFoundException

import scala.collection.mutable

// import com.databricks.spark.sql.perf.tpcds.Tables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.util.Utils



object OapBenchmarkDataBuilder extends OapPerfSuiteContext with Logging {

  private val defaultProperties = Map(
    "oap.benchmark.hdfs.file.root.dir"    -> "/dailytest",
    "oap.benchmark.tpcds.data.scale"      -> "200",
    "oap.benchmark.tpcds.data.partition"  -> "80",
    "oap.benchmark.tpcds.data.format"     -> "parquet"
  )

  def getDatabase(format: String) : String = {
    val dataScale = properties.get("oap.benchmark.tpcds.data.scale").get.toInt
    val baseName = format match {
      case "parquet" => s"parquet$dataScale"
      case "orc" => s"orc$dataScale"
      case _ => "default"
    }

    baseName
  }

  def formatTableLocation(rootDir: String, tableFormat: String): String = {
    s"${rootDir}/${getDatabase(tableFormat)}/"
  }

  private val properties = {
    try {
      new mutable.HashMap[String, String]() ++=
        Utils.getPropertiesFromFile("./src/test/oap-perf-suite/conf/oap-benchmark-default.conf")
    } catch {
      case e: IllegalArgumentException => {
        logWarning(e.getMessage + ". Use default setting!")
        defaultProperties
      }
    }

  }

  override def beforeAll(conf: Map[String, String] = Map.empty): Unit = {
    super.beforeAll(conf)
  }

//  def generateTables(): Unit = {
//    val versionNum = properties.get("oap.benchmark.support.oap.version").get
//    val codec = properties.get("oap.benchmark.compression.codec").get
//    val scale = properties.get("oap.benchmark.tpcds.data.scale").get.toInt
//    val partitions = properties.get("oap.benchmark.tpcds.data.partition").get.toInt
//    val hdfsRootDir = properties.get("oap.benchmark.hdfs.file.root.dir").get
//    val tpcdsToolPath = properties.get("oap.benchmark.tpcds.tool.dir").get
//    val dataFormats = properties.get("oap.benchmark.tpcds.data.format").get.split(",", 0)
//
//    dataFormats.foreach{ format =>
//      sqlContext.setConf(s"spark.sql.$format.compression.codec", codec)
//      val loc = formatTableLocation(hdfsRootDir, versionNum, format)
//      val tables = new Tables(sqlContext, tpcdsToolPath, scale)
//      tables.genData(
//        loc, format, true, false, true, false, false, "store_sales", partitions)
//    }
//  }
//
//  def generateDatabases() {
//    // TODO: get from OapFileFormatConfigSet
//    val dataFormats = properties.get("oap.benchmark.tpcds.data.format").get.split(",", 0)
//    dataFormats.foreach { format =>
//      spark.sql(s"create database if not exists ${getDatabase(format)}")
//    }
//
//    def genData(dataFormat: String) = {
//      val versionNum = properties.get("oap.benchmark.support.oap.version").get
//      val hdfsRootDir = properties.get("oap.benchmark.hdfs.file.root.dir").get
//      val dataLocation = formatTableLocation(hdfsRootDir, versionNum, dataFormat)
//
//      spark.sql(s"use ${getDatabase(dataFormat)}")
//      spark.sql("drop table if exists store_sales")
//      spark.sql("drop table if exists store_sales_dup")
//
//      /**
//       * To compare performance between B-Tree and Bitmap index, we generate duplicate
//       * tables of store_sales here. Besides, store_sales_dup table can be used in testing
//       * OAP strategies.
//       */
//      val df = spark.read.format(dataFormat).load(dataLocation + "store_sales")
//      val divRatio = df.select("ss_item_sk").orderBy(desc("ss_item_sk")).limit(1).
//        collect()(0)(0).asInstanceOf[Int] / 1000
//      val divideUdf = udf((s: Int) => s / divRatio)
//      df.withColumn("ss_item_sk1", divideUdf(col("ss_item_sk"))).write.format(dataFormat)
//        .mode(SaveMode.Overwrite).save(dataLocation + "store_sales1")
//
//      val conf = new Configuration()
//      val hadoopFs = FileSystem.get(conf)
//      hadoopFs.delete(new Path(dataLocation + "store_sales"), true)
//
//      // Notice here delete source flag should firstly be set to false
//      FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
//        hadoopFs, new Path(dataLocation + "store_sales"), false, conf)
//      FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
//        hadoopFs, new Path(dataLocation + "store_sales_dup"), true, conf)
//
//      sqlContext.createExternalTable("store_sales", dataLocation + "store_sales", dataFormat)
//      sqlContext.createExternalTable("store_sales_dup", dataLocation + "store_sales_dup"
//        , dataFormat)
//      logWarning(s"File size of original table store_sales in $dataFormats format: " +
//        TestUtil.calculateFileSize("store_sales", dataLocation, dataFormat)
//      )
//      logWarning("Records of table store_sales: " +
//        spark.read.format(dataFormat).load(dataLocation + "store_sales").count()
//      )
//    }
//
//    dataFormats.foreach(genData)
//  }
//
//  def buildAllIndex() {
//    def buildBtreeIndex(tablePath: String, table: String, attr: String): Unit = {
//      try {
//        spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
//      } catch {
//        case _: Throwable => logWarning("Index doesn't exist, so don't need to drop here!")
//      } finally {
//        TestUtil.time(
//          spark.sql(
//            s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BTREE"
//          ),
//          s"Create B-Tree index on ${table}(${attr}) cost "
//        )
//        logWarning(s"The size of B-Tree index on ${table}(${attr}) cost:" +
//          TestUtil.calculateIndexSize(table, tablePath, attr))
//      }
//    }
//
//    def buildBitmapIndex(tablePath: String, table: String, attr: String): Unit = {
//      try {
//        spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
//      } catch {
//        case _: Throwable => logWarning("Index doesn't exist, so don't need to drop here!")
//      } finally {
//        TestUtil.time(
//          spark.sql(
//            s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BITMAP"
//          ),
//          s"Create Bitmap index on ${table}(${attr}) cost"
//        )
//        logWarning(s"The size of Bitmap index on ${table}(${attr}) cost:" +
//          TestUtil.calculateIndexSize(table, tablePath, attr))
//      }
//    }
//
//    val versionNum = properties.get("oap.benchmark.support.oap.version").get
//    val hdfsRootDir = properties.get("oap.benchmark.hdfs.file.root.dir").get
//    val dataFormats = properties.get("oap.benchmark.tpcds.data.format").get.split(",", 0)
//
//    dataFormats.foreach { dataFormat => {
//        spark.sql(s"use ${getDatabase(dataFormat)}")
//        val tableLocation: String = formatTableLocation(hdfsRootDir, versionNum, dataFormat)
//        buildBtreeIndex(tableLocation, "store_sales", "ss_customer_sk")
//        buildBitmapIndex(tableLocation, "store_sales", "ss_item_sk1")
//      }
//    }
//  }
}
