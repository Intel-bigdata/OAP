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
package org.apache.spark.sql.execution.datasources.oap.benchmark

import org.apache.spark.sql.execution.datasources.oap.benchmark.BenchmarkUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure parquet with oap read performance for Indexed Sql.
 * To run this:
 * spark-submit --class <this class> --jars <spark sql test jar>
 */
object IndexedParquetReadBenchmark {

  def intScanBenchmark(values: Int): Unit = {

    // Benchmarks driving reader component directly.
    val benchmark = new Benchmark("Indexed Sql Single Int Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast(id as INT) as id from t1")
          .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")
        spark.sql("create oindex id_index on tempTable(id)").count()

        val min = 0
        val mid = values/2
        val max = values -1

        benchmark.addCase("SQL Parquet MR -> skipIndex") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
            OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key -> "true") {
            spark.sql(s"""select id from tempTable where id in ($min, $mid, $max)""").collect()
          }
        }

        benchmark.addCase("SQL Parquet Vectorized -> skipIndex") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
            OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key -> "true") {
            spark.sql(s"""select id from tempTable where id in ($min, $mid, $max)""").collect()
          }
        }

        benchmark.addCase("SQL Parquet MR -> force useIndex") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
            OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key -> "false") {
            spark.sql(s"""select id from tempTable where id in ($min, $mid, $max)""").collect()
          }
        }

        benchmark.addCase("SQL Parquet Vectorized -> force useIndex") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
            OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key -> "false") {
            spark.sql(s"""select id from tempTable where id in ($min, $mid, $max)""").collect()
          }
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_144-b01 on Linux 2.6.32_1-18-0-0
        Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz
        Indexed Sql Single Int Column Scan:      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL Parquet MR -> skipIndex                   2553 / 2577          6.2         162.3       1.0X
        SQL Parquet Vectorized -> skipIndex            189 /  198         83.2          12.0      13.5X
        SQL Parquet MR -> force useIndex               578 /  581         27.2          36.7       4.4X
        SQL Parquet Vectorized -> force useIndex       158 /  168         99.5          10.0      16.2X
        */
        benchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    intScanBenchmark(1024 * 1024 * 15)
  }
}
