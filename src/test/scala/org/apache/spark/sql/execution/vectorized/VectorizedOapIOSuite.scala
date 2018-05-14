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

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{IndexedVectorizedOapRecordReader, VectorizedOapRecordReader}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupportWrapper, ParquetTest, SpecificParquetRecordReaderBase}
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.unsafe.types.UTF8String

class VectorizedOapIOSuite extends QueryTest with ParquetTest with SharedOapContext {
  import testImplicits._

  test("VectorizedOapRecordReader - direct path read") {
    val data = (0 to 10).map(i => (i, (i + 'a').toChar.toString))
    withTempPath { dir =>
      val df = spark.createDataFrame(data)
      val schema = df.schema.json
      df.repartition(1).write.parquet(dir.getCanonicalPath)
      val file = SpecificParquetRecordReaderBase.listDirectory(dir).get(0)
      val path = new Path(file.asInstanceOf[String])

      {
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, schema)
        val reader = new VectorizedOapRecordReader(path, configuration)
        try {
          reader.initialize()
          val result = mutable.ArrayBuffer.empty[(Int, String)]
          while (reader.nextKeyValue()) {
            val row = reader.getCurrentValue.asInstanceOf[InternalRow]
            val v = (row.getInt(0), row.getString(1))
            result += v
          }
          assert(data == result)
          configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
        } finally {
          reader.close()
        }

      }

      // Project just one column
    {
      val requestSchema = requestSchemaString(df.schema, Array(1))
      configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema)
      val reader = new VectorizedOapRecordReader(path, configuration)
      try {
        reader.initialize()
        val result = mutable.ArrayBuffer.empty[(String)]
        while (reader.nextKeyValue()) {
          val row = reader.getCurrentValue.asInstanceOf[InternalRow]
          result += row.getString(0)
        }
        assert(data.map(_._2) == result)
        configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
      } finally {
        reader.close()
      }
    }

      // Project columns in opposite order
    {
      val requestSchema = requestSchemaString(df.schema, Array(1, 0))
      configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema)
      val reader = new VectorizedOapRecordReader(path, configuration)
      try {
        reader.initialize()
        val result = mutable.ArrayBuffer.empty[(String, Int)]
        while (reader.nextKeyValue()) {
          val row = reader.getCurrentValue.asInstanceOf[InternalRow]
          val v = (row.getString(0), row.getInt(1))
          result += v
        }
        assert(data.map { x => (x._2, x._1) } == result)
        configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
      } finally {
        reader.close()
      }
    }
      // Empty projection
    {
      val requestSchema = requestSchemaString(df.schema, Array())
      configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema)
      val reader = new VectorizedOapRecordReader(path, configuration)
      try {
        reader.initialize()
        var result = 0
        while (reader.nextKeyValue()) {
          result += 1
        }
        assert(result == data.length)
        configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
      } finally {
        reader.close()
      }
    }
    }
  }

  test("VectorizedParquetRecordReader - partition column types") {
    withTempPath { dir =>
      Seq(1).toDF().repartition(1).write.parquet(dir.getCanonicalPath)

      val dataTypes =
        Seq(StringType, BooleanType, ByteType, ShortType, IntegerType, LongType,
          FloatType, DoubleType, DecimalType(25, 5), DateType, TimestampType)

      val constantValues =
        Seq(
          UTF8String.fromString("a string"),
          true,
          1.toByte,
          2.toShort,
          3,
          Long.MaxValue,
          0.25.toFloat,
          0.75D,
          Decimal("1234.23456"),
          DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2015-01-01")),
          DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf("2015-01-01 23:50:59.123")))

      dataTypes.zip(constantValues).foreach { case (dt, v) =>
        val schema = StructType(StructField("pcol", dt) :: Nil)
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, schema.json)
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).get(0)
        val path = new Path(file)
        val vectorizedReader = new VectorizedOapRecordReader(path, configuration)
        val partitionValues = new GenericInternalRow(Array(v))


        try {
          vectorizedReader.initialize()
          vectorizedReader.initBatch(schema, partitionValues)
          vectorizedReader.nextKeyValue()
          val row = vectorizedReader.getCurrentValue.asInstanceOf[InternalRow]

          // Use `GenericMutableRow` by explicitly copying rather than `ColumnarBatch`
          // in order to use get(...) method which is not implemented in `ColumnarBatch`.
          val actual = row.copy().get(1, dt)
          val expected = v
          assert(actual == expected)
          configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
        } finally {
          vectorizedReader.close()
        }
      }
    }
  }

  test("IndexedVectorizedOapRecordReader - direct path read") {
    val data = (0 to 6000).map(i => (i, (i + 'a').toChar.toString))
    val rowIds = Array(1, 1001, 2001, 3001, 4001, 5001)
    val expected = data.filter( item => rowIds.contains(item._1))
    withTempPath { dir =>
      val df = spark.createDataFrame(data)
      val schema = df.schema.json
      df.repartition(1).write.parquet(dir.getCanonicalPath)
      val file = SpecificParquetRecordReaderBase.listDirectory(dir).get(0)
      val path = new Path(file.asInstanceOf[String])

      {
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, schema)
        val reader = new IndexedVectorizedOapRecordReader(path, configuration, rowIds)
        try {
          reader.initialize()
          val result = mutable.ArrayBuffer.empty[(Int, String)]
          while (reader.nextKeyValue()) {
            val row = reader.getCurrentValue.asInstanceOf[InternalRow]
            val v = (row.getInt(0), row.getString(1))
            result += v
          }
          assert(expected == result)
        } finally {
          reader.close()
          configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
        }
      }

        // Project just one column
      {
        val requestSchema = requestSchemaString(df.schema, Array(1))
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema)
        val reader = new IndexedVectorizedOapRecordReader(path, configuration, rowIds)
        try {
          reader.initialize()
          val result = mutable.ArrayBuffer.empty[(String)]
          while (reader.nextKeyValue()) {
            val row = reader.getCurrentValue.asInstanceOf[InternalRow]
            result += row.getString(0)
          }
          assert(expected.map(_._2) == result)
        } finally {
          reader.close()
          configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
        }
      }

        // Project columns in opposite order
      {
        val requestSchema = requestSchemaString(df.schema, Array(1, 0))
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema)
        val reader = new IndexedVectorizedOapRecordReader(path, configuration, rowIds)
        try {
          reader.initialize()
          val result = mutable.ArrayBuffer.empty[(String, Int)]
          while (reader.nextKeyValue()) {
            val row = reader.getCurrentValue.asInstanceOf[InternalRow]
            val v = (row.getString(0), row.getInt(1))
            result += v
          }
          assert(expected.map { x => (x._2, x._1) } == result)
        } finally {
          reader.close()
          configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
        }
      }
        // Empty projection
      {
        val requestSchema = requestSchemaString(df.schema, Array())
        configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchema)
        val reader = new IndexedVectorizedOapRecordReader(path, configuration, rowIds)
        try {
          reader.initialize()
          var result = 0
          while (reader.nextKeyValue()) {
            result += 1
          }
          assert(result == expected.length)
        } finally {
          reader.close()
          configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
        }
      }
    }
  }

  private def requestSchemaString(schema: StructType, requiredIds: Array[Int]): String = {
    var requestSchema = new StructType
    for (index <- requiredIds) {
      requestSchema = requestSchema.add(schema(index))
    }
    requestSchema.json
  }

}
