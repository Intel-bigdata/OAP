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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{DefaultRecordReader, VectorizedOapRecordReader}

import org.apache.spark.sql.execution.datasources.oap.benchmark.BenchmarkUtils._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure parquet with oap read performance for StringWithNullsScan
 * and IntScan.
 * To run this:
 * spark-submit --class <this class> --jars <spark sql test jar>
 */
object FullScanParquetReaderBenchmark {

  def stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql(s"select IF(rand(1) < $fractionOfNulls, NULL, cast(id as STRING)) as c1, " +
          s"IF(rand(2) < $fractionOfNulls, NULL, cast(id as STRING)) as c2 from t1")
          .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")
        val benchmark = new Benchmark("String with Nulls Scan", values)
        val configuration = new Configuration()
        val requestSchema = new StructType()
          .add(StructField("c1", StringType))
          .add(StructField("c2", StringType))
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema.json)
        configuration.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
        configuration.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, false)
        configuration.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, false)
        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray

        benchmark.addCase("Oap Parquet MR") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val readSupport = new ParquetReadSupportWrapper
            val reader =
              new DefaultRecordReader(readSupport, new Path(p), configuration, null)
            try {
              reader.initialize()
              while (reader.nextKeyValue()) {
                val row = reader.getCurrentValue
                val value = row.getUTF8String(0)
                if (!row.isNullAt(0) && !row.isNullAt(1)) sum += value.numBytes()
              }
            } finally {
              reader.close()
            }
          }
        }

        benchmark.addCase("Oap Parquet Vectorized") { _ =>
          var sum = 0
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedOapRecordReader(new Path(p), configuration, null)
            try {
              reader.initialize()
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val rowIterator = batch.rowIterator()
                while (rowIterator.hasNext) {
                  val row = rowIterator.next()
                  val value = row.getUTF8String(0)
                  if (!row.isNullAt(0) && !row.isNullAt(1)) sum += value.numBytes()
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        benchmark.addCase("Parquet Vectorized") { _ =>
          var sum = 0
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("c1" :: "c2" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val rowIterator = batch.rowIterator()
                while (rowIterator.hasNext) {
                  val row = rowIterator.next()
                  val value = row.getUTF8String(0)
                  if (!row.isNullAt(0) && !row.isNullAt(1)) sum += value.numBytes()
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        benchmark.addCase("Oap Parquet Vectorized (Null Filtering)") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedOapRecordReader(new Path(p), configuration, null)
            try {
              reader.initialize()
              val batch = reader.resultBatch()
              batch.filterNullsInColumn(0)
              batch.filterNullsInColumn(1)
              while (reader.nextBatch()) {
                val rowIterator = batch.rowIterator()
                while (rowIterator.hasNext) {
                  sum += rowIterator.next().getUTF8String(0).numBytes()
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        benchmark.addCase("Parquet Vectorized (Null Filtering)") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("c1" :: "c2" :: Nil).asJava)
              val batch = reader.resultBatch()
              batch.filterNullsInColumn(0)
              batch.filterNullsInColumn(1)
              while (reader.nextBatch()) {
                val rowIterator = batch.rowIterator()
                while (rowIterator.hasNext) {
                  sum += rowIterator.next().getUTF8String(0).numBytes()
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        /*
        fractionOfNulls = 0.95
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_144-b01 on Linux 2.6.32_1-18-0-0
        Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz
        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Oap Parquet MR                                1188 / 1193          8.8         113.3       1.0X
        Oap Parquet Vectorized                         172 /  189         61.1          16.4       6.9X
        Parquet Vectorized                             184 /  205         56.9          17.6       6.4X
        Oap Parquet Vectorized (Null Filtering)        171 /  173         61.4          16.3       7.0X
        Parquet Vectorized (Null Filtering)            183 /  187         57.2          17.5       6.5X

        fractionOfNulls = 0.50
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_144-b01 on Linux 2.6.32_1-18-0-0
        Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz
        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Oap Parquet MR                                1922 / 1955          5.5         183.3       1.0X
        Oap Parquet Vectorized                         876 /  887         12.0          83.6       2.2X
        Parquet Vectorized                             897 /  938         11.7          85.5       2.1X
        Oap Parquet Vectorized (Null Filtering)        902 /  932         11.6          86.1       2.1X
        Parquet Vectorized (Null Filtering)            912 /  940         11.5          86.9       2.1X

        fractionOfNulls = 0.00
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_144-b01 on Linux 2.6.32_1-18-0-0
        Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz
        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Oap Parquet MR                                2346 / 2368          4.5         223.8       1.0X
        Oap Parquet Vectorized                        1120 / 1130          9.4         106.8       2.1X
        Parquet Vectorized                            1134 / 1138          9.3         108.1       2.1X
        Oap Parquet Vectorized (Null Filtering)       1005 / 1023         10.4          95.8       2.3X
        Parquet Vectorized (Null Filtering)           1044 / 1062         10.0          99.5       2.2X
        */
        benchmark.run()
      }
    }
  }

  def intScanBenchmark(values: Int): Unit = {

    // Benchmarks driving reader component directly.
    val readerBenchmark = new Benchmark("Parquet Reader Single Int Column Scan", values)
    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast(id as INT) as id from t1")
          .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")
        val configuration = new Configuration()
        val requestSchema = new StructType().add(StructField("id", IntegerType))
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema.json)
        configuration.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
        configuration.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, false)
        configuration.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, false)
        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray

        // Driving the parquet reader in batch mode directly.
        readerBenchmark.addCase("Oap ParquetReader MR") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val readSupport = new ParquetReadSupportWrapper

            val reader =
              new DefaultRecordReader(readSupport, new Path(p), configuration, null)
            try {
              reader.initialize()
              while (reader.nextKeyValue()) {
                val record = reader.getCurrentValue
                if (!record.isNullAt(0)) sum += record.getInt(0)
              }
            } finally {
              reader.close()
            }
          }
        }

        readerBenchmark.addCase("OapParquetReader Vectorized -> Batch") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedOapRecordReader(new Path(p), configuration, null)
            try {
              reader.initialize()
              val batch = reader.resultBatch()
              val col = batch.column(0)
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!col.isNullAt(i)) sum += col.getInt(i)
                  i += 1
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        readerBenchmark.addCase("ParquetReader Vectorized -> Batch") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              val col = batch.column(0)
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!col.isNullAt(i)) sum += col.getInt(i)
                  i += 1
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        // Decoding in vectorized but having the reader return rows.
        readerBenchmark.addCase("OapParquetReader Vectorized -> Row") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedOapRecordReader(new Path(p), configuration, null)
            try {
              reader.initialize()
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val it = batch.rowIterator()
                while (it.hasNext) {
                  val record = it.next()
                  if (!record.isNullAt(0)) sum += record.getInt(0)
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        readerBenchmark.addCase("ParquetReader Vectorized -> Row") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val it = batch.rowIterator()
                while (it.hasNext) {
                  val record = it.next()
                  if (!record.isNullAt(0)) sum += record.getInt(0)
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_144-b01 on Linux 2.6.32_1-18-0-0
        Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz
        Parquet Reader Single Int Column Scan:   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Oap ParquetReader MR                          1544 / 1583         10.2          98.2       1.0X
        OapParquetReader Vectorized  -> Batch         130 /  155         121.4           8.2      11.9X
        ParquetReader Vectorized     -> Batch         142 /  169         111.2           9.0      10.9X
        OapParquetReader Vectorized  -> Row           217 /  255          72.6          13.8       7.1X
        ParquetReader Vectorized     -> Row           235 /  265          66.9          14.9       6.6X
        */
        readerBenchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    for (fractionOfNulls <- List(0.95, 0.50, 0.0)) {
      stringWithNullsScanBenchmark(1024 * 1024 * 10, fractionOfNulls)
    }
    intScanBenchmark(1024 * 1024 * 15)
  }
}
