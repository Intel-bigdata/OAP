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
package org.apache.spark.sql.execution.vectorized

import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat, VectorizedOapRecordReader}

import org.apache.spark.sql.execution.datasources.parquet.{ParquetCompatibilityTest, ParquetReadSupportHelper, SpecificParquetRecordReaderBase}
import org.apache.spark.sql.test.oap.SharedOapContext

class VectorizedOapEncodingSuite extends ParquetCompatibilityTest with SharedOapContext{

  import testImplicits._

  private val ROW = (1.toByte, 2, 3L, "abc")
  private val NULL_ROW = (
    null.asInstanceOf[java.lang.Byte],
    null.asInstanceOf[Integer],
    null.asInstanceOf[java.lang.Long],
    null.asInstanceOf[String])

  test("All Types Dictionary") {
    (1 :: 1000 :: Nil).foreach { n => {
      withTempPath { dir =>
        val df = List.fill(n)(ROW).toDF
        df.repartition(1).write.parquet(dir.getCanonicalPath)
        val schema = df.schema.json
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head
        val path = new Path(file.asInstanceOf[String])
        val footer = ParquetFileReader.readFooter(configuration, path, NO_FILTER)
        (footer :: null :: Nil).foreach { f =>
          configuration.set(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA, schema)
          val reader = new VectorizedOapRecordReader(path, configuration, f)
          reader.initialize()
          val batch = reader.resultBatch()
          assert(reader.nextBatch())
          assert(batch.numRows() == n)
          var i = 0
          while (i < n) {
            assert(!batch.isFiltered(i))
            assert(batch.column(0).getByte(i) == 1)
            assert(batch.column(1).getInt(i) == 2)
            assert(batch.column(2).getLong(i) == 3)
            assert(batch.column(3).getUTF8String(i).toString == "abc")
            i += 1
          }
          configuration.unset(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA)
          reader.close()
        }
      }
    }
    }
  }

  test("All Types Null") {
    (1 :: 100 :: Nil).foreach { n => {
      withTempPath { dir =>
        val df = List.fill(n)(NULL_ROW).toDF
        df.repartition(1).write.parquet(dir.getCanonicalPath)
        val schema = df.schema.json
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head
        val path = new Path(file.asInstanceOf[String])
        val footer = ParquetFileReader.readFooter(configuration, path, NO_FILTER)
        (footer :: null :: Nil).foreach { f =>
          configuration.set(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA, schema)
          val reader = new VectorizedOapRecordReader(path, configuration, f)
          reader.initialize()
          val batch = reader.resultBatch()
          assert(reader.nextBatch())
          assert(batch.numRows() == n)
          var i = 0
          while (i < n) {
            assert(!batch.isFiltered(i))
            assert(batch.column(0).isNullAt(i))
            assert(batch.column(1).isNullAt(i))
            assert(batch.column(2).isNullAt(i))
            assert(batch.column(3).isNullAt(i))
            i += 1
          }
          configuration.unset(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA)
          reader.close()
        }
      }
    }
    }
  }

  test("Read row group containing both dictionary and plain encoded pages") {
    withSQLConf(ParquetOutputFormat.DICTIONARY_PAGE_SIZE -> "2048",
      ParquetOutputFormat.PAGE_SIZE -> "4096") {
      withTempPath { dir =>
        val data = (0 until 512).flatMap(i => Seq.fill(3)(i.toString))
        val df = data.toDF("f")
        df.coalesce(1).write.parquet(dir.getCanonicalPath)
        val schema = df.schema.json
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head
        val path = new Path(file.asInstanceOf[String])
        val footer = ParquetFileReader.readFooter(configuration, path, NO_FILTER)

        (footer :: null:: Nil).foreach { f =>
          configuration.set(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA, schema)
          val reader = new VectorizedOapRecordReader(path, configuration, f)
          reader.initialize()
          val batch = reader.resultBatch()
          val column = batch.column(0)
          assert(reader.nextBatch())

          (0 until 512).foreach { i =>
            assert(!batch.isFiltered(i))
            assert(column.getUTF8String(3 * i).toString == i.toString)
            assert(column.getUTF8String(3 * i + 1).toString == i.toString)
            assert(column.getUTF8String(3 * i + 2).toString == i.toString)
          }
          configuration.unset(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA)
          reader.close()
        }
      }
    }
  }
}
