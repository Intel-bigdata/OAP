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

import org.apache.spark.sql.SparkSession

object IndexBuilderApp {
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
    val dataScale = args(2)
    // 7=111 to test all indexes, 5=101 to test B-tree and trie, 6=110 to test B-tree and Bitmap
    val indexFlag = args(3).toInt
    val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0").
      enableHiveSupport().getOrCreate()
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")

    def buildBtreeIndex(tablePath: String): Unit = {
      val table = "store_sales"
      val attr = "ss_ticket_number"
      try {
        spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
      } catch {
        case _ => println("Index doesn't exist, so don't need to drop here!")
      } finally {
        TestUtil.time(
          spark.sql(
            s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BTREE"
          ),
          s"Create B-Tree index on $table cost "
        )
        println(s"The size of B-Tree index on $table cost :" +
          TestUtil.calculateIndexSize(table, tablePath, attr))
      }
    }

    def buildTrieIndex(tablePath: String): Unit = {
      val table = "customer"
      val attr = "c_email_address"
      try {
        spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
      } catch {
        case _ => println("Index doesn't exist, so don't need to drop here!")
      } finally {
        TestUtil.time(
          spark.sql(
            s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr)"
          ),
          s"Create Trie index on $table cost"
        )
        println(s"The size of Trie index on $table cost :" +
          TestUtil.calculateIndexSize(table, tablePath, attr))
      }
    }

    def buildBitmapIndex(tablePath: String): Unit = {
      val table = "store_sales"
      val attr = "ss_item_sk1"
      try {
        spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
      } catch {
        case _ => println("Index doesn't exist, so don't need to drop here!")
      } finally {
        TestUtil.time(
          spark.sql(
            s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BITMAP"
          ),
          s"Create Bitmap index on $table cost"
        )
        println(s"The size of Bitmap index on $table cost :" +
          TestUtil.calculateIndexSize(table, tablePath, attr))
      }
    }

    dataFormats.foreach(dataFormat => {
      spark.sql(s"USE ${dataFormat}tpcds${dataScale}")
      val tablePath: String = "hdfs:///user/oap/oaptest/" +
        s"oap-0.${versionNum}.0/tpcds/tpcds${dataScale}/${dataFormat}/"
      if((indexFlag & 4) > 0) buildBtreeIndex(tablePath)
      if((indexFlag & 2) > 0) buildBitmapIndex(tablePath)
      if((indexFlag & 1) > 0) buildTrieIndex(tablePath)
    })
    spark.stop()
  }
}
