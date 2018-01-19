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

import scala.collection.mutable.{ArrayBuffer, HashMap}

import sys.process._

import org.apache.spark.sql.SparkSession

object BenchmarkTest {
  def main(args: Array[String]) {
    if (args.length < 8) {
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

    // 3=11 for all tests, 1=01 for baseline tests, 2=10 for indexes tests
    val useIndexes: Seq[Boolean] = args(3).toInt match {
      case 3 => Seq(true, false)
      case 2 => Seq(true)
      case 1 => Seq(false)
      case _ => Seq()
    }

    // 7=111 to test all indexes, 5=101 to test B-tree and trie, 6=110 to test B-tree and Bitmap
    val indexFlags = args(4).toInt

    // enable to test Oap strategies
    val testStrategy = args(5)

    // enable use of statistics manager, this doesn't impact oap strategies queries
    val enableStatistics = args(6)

    val testTimes = args(7).toInt

    if(testTimes < 1) sys.error("Test times should be positive!")

    def getSession(useIndex: Boolean): SparkSession = {
      val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0")
        .enableHiveSupport().getOrCreate()
      spark.sqlContext.setConf("spark.sql.oap.oindex.enabled", s"${useIndex}")
      spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
      spark.sqlContext.setConf("spark.sql.oap.oindex.eis.enabled", enableStatistics)
      spark
    }

    def cleanAfterEach(spark: SparkSession) = {
      spark.stop()
      assert(("rm -f ./metastore_db/db.lck" !) == 0)
      assert(("rm -f ./metastore_db/dbex.lck" !) == 0)
      assert(("sync" !) == 0)
      var dropCacheCmd: Seq[String] = Seq("bash", "-c", "echo 3 > /proc/sys/vm/drop_caches")
      assert(dropCacheCmd.! == 0)
    }

    def testForBtreeIndex(spark: SparkSession): ArrayBuffer[Int] = {
      // B-tree index on $d table store_sales (ss_ticket_number)
      val table = "store_sales"
      val attr = "ss_ticket_number"
      implicit val resList = ArrayBuffer[Int]()
      // Single column query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 2000000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 100000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 6000000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 200")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 400")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 800")
        .foreach{ _ => })
      // Two columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 2000000" +
        s" AND ss_customer_sk >= 120000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 100000 " +
        s"AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000 " +
        s"AND ss_net_paid > 100.0 AND ss_net_paid < 200.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000 " +
        s"AND ss_net_paid BETWEEN 100.0 AND 110.0").foreach{ _ => })
      // Three columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 2000000 AND" +
        s" ss_customer_sk >= 120000 AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 100000 AND " +
        s"ss_list_price < 100.0 AND ss_net_paid > 500.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000 AND " +
        s"ss_net_paid > 100.0 AND ss_net_paid < 110.0 AND ss_list_price < 100.0").foreach{ _ => })
      resList
    }

    def testOapStrategy(spark: SparkSession): ArrayBuffer[Int] = {
      val table = "store_sales"
      val btreeIndexAttr = "ss_ticket_number"
      val bitmapIndexAttr = "ss_item_sk1"
      val lsRange = (1 to 10).mkString(",")
      val msRange = (1 to 5).mkString(",")
      implicit val resList = ArrayBuffer[Int]()
      // OapSortLimitStrategy query, this works for B-Tree only
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $btreeIndexAttr > 100 AND" +
        s" $btreeIndexAttr < 1000 ORDER BY $btreeIndexAttr LIMIT 100").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table ORDER BY $btreeIndexAttr" +
        " LIMIT 10000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $btreeIndexAttr BETWEEN 3000 AND" +
        s" 20000 ORDER BY $btreeIndexAttr LIMIT 100000").foreach{ _ => })
      // OAPSemiJoinStrategy query, this works for Bitmap only
      TestUtil.queryTime(
        spark.sql(
          s"SELECT * FROM $table s1 WHERE EXISTS " +
            s"(SELECT * FROM $table s2 WHERE s1.$bitmapIndexAttr = s2.$bitmapIndexAttr " +
            s"AND s1.$bitmapIndexAttr IN ( $lsRange ))"
        ).foreach{ _ => }
      )
      TestUtil.queryTime(
        spark.sql(
          s"SELECT * FROM $table s1 WHERE EXISTS " +
            s"(SELECT * FROM $table s2 WHERE s1.$bitmapIndexAttr = s2.$bitmapIndexAttr " +
            s"AND s1.$bitmapIndexAttr IN ( $msRange ))"
        ).foreach{ _ => }
      )
      TestUtil.queryTime(
        spark.sql(
          s"SELECT * FROM $table s1 WHERE EXISTS " +
            s"(SELECT * FROM $table s2 WHERE s1.$bitmapIndexAttr = s2.$bitmapIndexAttr " +
            s"AND s1.$bitmapIndexAttr = 25)"
        ).foreach{ _ => }
      )
      // OapGroupAggregateStrategy query, this works for both
      TestUtil.queryTime(
        spark.sql(
          s"SELECT max(${btreeIndexAttr}) FROM $table " +
            s"WHERE $bitmapIndexAttr IN ( $msRange ) AND " +
            s"$btreeIndexAttr BETWEEN 1 AND 10000 " +
            s"GROUP BY $bitmapIndexAttr"
        ).foreach{ _ => }
      )
      TestUtil.queryTime(
        spark.sql(
          s"SELECT max(${bitmapIndexAttr}) FROM $table " +
            s"WHERE $bitmapIndexAttr = 20 AND " +
            s"$btreeIndexAttr BETWEEN 1 AND 10000 " +
            s"GROUP BY $btreeIndexAttr"
        ).foreach{ _ => }
      )
      resList
    }

    def testForTrieIndex(spark: SparkSession): ArrayBuffer[Int] = {
      val table = "customer"
      val attr = "c_email_address"
      implicit val resList = ArrayBuffer[Int]()
      // Single column query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE '%.org'")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'P%.edu'")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'Irene%'")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = " +
        s"'Charles.Swanson@SuOeUpKkthHS3.org'").foreach{ _ => })

      // Two columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE '%.org'" +
        s" AND c_birth_year > 1978").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'P%.edu' " +
        s"AND c_birth_month > 4").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'Irene%' " +
        s"AND c_birth_country = 'BRAZIL'").foreach{ _ => })

      // Three columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE '%.org' " +
        s"AND c_birth_year > 1978 AND c_birth_day BETWEEN 4 AND 10").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'P%.edu' " +
        s"AND c_birth_month > 4 AND c_birth_country = 'BRAZIL'").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'Irene%' " +
        s"AND c_birth_country = 'MEXICO' AND c_birth_year > 1978").foreach{ _ => })
      resList
    }

    def testForBitmapIndex(spark: SparkSession): ArrayBuffer[Int] = {
      // Bitmap index on $d table store_sales (ss_item_sk)
      val table = "store_sales"
      val attr = "ss_item_sk1"
      implicit val resList = ArrayBuffer[Int]()
      val lsRange = (1 to 10).mkString(",")
      val msRange = (1 to 5).mkString(",")

      // Single column query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $lsRange )")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $msRange )")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 10").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 25").foreach{ _ => })

      // Two columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $lsRange )" +
        s" AND ss_customer_sk >= 120000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $msRange ) " +
        s"AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 10 " +
        s"AND ss_net_paid > 100.0 AND ss_net_paid < 200.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 25 " +
        s"AND ss_net_paid > 100.0 AND ss_net_paid < 200.0").foreach{ _ => })

      // Three columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $lsRange ) " +
        s"AND ss_customer_sk >= 120000 AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $msRange ) " +
        s"AND ss_list_price < 100.0 AND ss_net_paid > 500.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 10 AND " +
        s"ss_net_paid > 100.0 AND ss_net_paid < 200.0 AND ss_list_price < 100.0").foreach{ _ => })
      resList
    }

    // basic queries for data format + index type + use index / baseline
    Array(1, 2, 4).foreach(indexMask => {
      if ((indexMask & indexFlags) > 0) {
        println(indexMask match {
          case 1 => "Test results of Trie index:"
          case 2 => "Test results of Bitmap index:"
          case 4 => "Test results of Btree index:"
        })
        val resMap = HashMap[String, Seq[ArrayBuffer[Int]]]()
        var queryNums = 0
        dataFormats.foreach(dataFormat => {
          useIndexes.foreach(useIndex => {
            val spark = getSession(useIndex)
            spark.sql(s"USE ${dataFormat}tpcds${dataScale}")
            resMap.put(s"${dataFormat}-${useIndex}", indexMask match {
              case 1 => (1 to testTimes).map(_ => testForTrieIndex(spark))
              case 2 => (1 to testTimes).map(_ => testForBitmapIndex(spark))
              case _ => (1 to testTimes).map(_ => testForBtreeIndex(spark))
            })
            queryNums = resMap.get(s"${dataFormat}-${useIndex}").get(0).size
            cleanAfterEach(spark)
          })
        })
        val res = dataFormats.flatMap(dataFormat =>
          useIndexes.map(useIndex =>
            (s"${dataFormat}-${if (useIndex) "with-index" else "without-index"}",
              resMap.get(s"${dataFormat}-${useIndex}").get)
          )
        )
        TestUtil.formatResults(res, queryNums, testTimes)
      }
    })

    /**
     * We compare the performance under following three settings:
     * 1）Disable index selection
     * 2）Enable index selection but disable file and stats policy
     * 3) Enable index selection and both two policies (default mode)
     */
    if (testStrategy == "true" && dataFormats.contains("oap")) {
      println("Test results of Oap Strategies:")
      val dataFormat = "oap"
      val resMap = HashMap[String, Seq[ArrayBuffer[Int]]]()
      var queryNums = 0
      val testOptions = Seq(
        ("eis_disabled", "false", "false", "false"),
        ("eis_enabled_policies_disabled", "true", "false", "false"),
        ("all_enabled", "true", "true", "true")
      )
      testOptions.foreach(option => {
        val spark = getSession(true)
        spark.sqlContext.setConf("spark.sql.oap.oindex.eis.enabled", option._2)
        spark.sqlContext.setConf("spark.sql.oap.oindex.file.policy", option._3)
        spark.sqlContext.setConf("spark.sql.oap.oindex.statistics.policy", option._4)
        spark.sql(s"USE ${dataFormat}tpcds${dataScale}")
        resMap.put(option._1, (1 to testTimes)
          .map(_ => testOapStrategy(spark)))
        queryNums = resMap.get(option._1).get(0).size
        cleanAfterEach(spark)
      })
      val res = testOptions.map(option => (option._1, resMap.get(option._1).get))
      TestUtil.formatResults(res, queryNums, testTimes)
    }
  }
}
