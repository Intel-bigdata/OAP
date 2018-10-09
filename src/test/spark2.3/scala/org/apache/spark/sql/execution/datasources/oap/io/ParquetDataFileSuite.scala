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

package org.apache.spark.sql.execution.datasources.oap.io

import java.io.{File, IOException}
import java.util
import java.util.TimeZone

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.column.ParquetProperties.WriterVersion.{PARQUET_1_0, PARQUET_2_0}
import org.apache.parquet.example.Paper
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{SimpleGroup, SimpleGroupFactory}
import org.apache.parquet.hadoop.ParquetFiberDataReader
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.REQUIRED
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupportWrapper, VectorizedColumnReader, VectorizedColumnReaderWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.oap.adapter.ColumnVectorAdapter
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

abstract class ParquetDataFileSuite extends SparkFunSuite with SharedOapContext
  with BeforeAndAfterEach with Logging {

  protected val fileDir: File = Utils.createTempDir()

  protected val fileName: String = Utils.tempFileWith(fileDir).getAbsolutePath

  protected def data: Seq[Group]

  protected def parquetSchema: MessageType

  protected def dataVersion: WriterVersion

  override def beforeEach(): Unit = {
    configuration.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key,
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get)
    configuration.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get)
    configuration.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get)
    // SQLConf.PARQUET_INT64_AS_TIMESTAMP_MILLIS is defined in Spark 2.2 and later
    configuration.setBoolean("spark.sql.parquet.int64AsTimestampMillis", false)
    prepareData()
  }

  override def afterEach(): Unit = {
    configuration.unset(SQLConf.PARQUET_BINARY_AS_STRING.key)
    configuration.unset(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key)
    configuration.unset(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key)
    configuration.unset("spark.sql.parquet.int64AsTimestampMillis")
    cleanDir()
  }

  private def prepareData(): Unit = {
    val dictPageSize = 512
    val blockSize = 128 * 1024
    val pageSize = 1024
    GroupWriteSupport.setSchema(parquetSchema, configuration)
    val writer = ExampleParquetWriter.builder(new Path(fileName))
      .withCompressionCodec(UNCOMPRESSED)
      .withRowGroupSize(blockSize)
      .withPageSize(pageSize)
      .withDictionaryPageSize(dictPageSize)
      .withDictionaryEncoding(true)
      .withValidation(false)
      .withWriterVersion(dataVersion)
      .withConf(configuration)
      .build()

    data.foreach(writer.write)
    writer.close()
  }

  private def cleanDir(): Unit = {
    val path = new Path(fileName)
    val fs = path.getFileSystem(configuration)
    if (fs.exists(path.getParent)) {
      fs.delete(path.getParent, true)
    }
  }
}

class SimpleDataSuite extends ParquetDataFileSuite {

  private val requestSchema: StructType = new StructType()
    .add(StructField("int32_field", IntegerType))
    .add(StructField("int64_field", LongType))
    .add(StructField("boolean_field", BooleanType))
    .add(StructField("float_field", FloatType))

  override def parquetSchema: MessageType = new MessageType("test",
    new PrimitiveType(REQUIRED, INT32, "int32_field"),
    new PrimitiveType(REQUIRED, INT64, "int64_field"),
    new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field"),
    new PrimitiveType(REQUIRED, FLOAT, "float_field"),
    new PrimitiveType(REQUIRED, DOUBLE, "double_field")
  )

  override def dataVersion: WriterVersion = PARQUET_2_0

  override def data: Seq[Group] = {
    val factory = new SimpleGroupFactory(parquetSchema)
    (0 until 1000).map(i => factory.newGroup()
      .append("int32_field", i)
      .append("int64_field", 64L)
      .append("boolean_field", true)
      .append("float_field", 1.0f)
      .append("double_field", 2.0d))
  }

  test("read by columnIds and rowIds") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 1)
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[Int]()
    while (iterator.hasNext) {
      val row = iterator.next
      assert(row.numFields == 2)
      result += row.getInt(0)
    }
    iterator.close()
    assert(rowIds.length == result.length)
    for (i <- rowIds.indices) {
      assert(rowIds(i) == result(i))
    }
  }

  test("read by columnIds and empty rowIds array") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0, 1)
    val rowIds = Array.emptyIntArray
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    assert(!iterator.hasNext)
    val e = intercept[java.util.NoSuchElementException] {
      iterator.next()
    }.getMessage
    iterator.close()
    assert(e.contains("next on empty iterator"))
  }

  test("read by columnIds ") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val requiredIds = Array(0)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[ Int ]()
    while (iterator.hasNext) {
      val row = iterator.next
      result += row.getInt(0)
    }
    iterator.close()
    val length = data.length
    assert(length == result.length)
    for (i <- 0 until length) {
      assert(i == result(i))
    }
  }

  test("getDataFileMeta") {
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    val meta = reader.getDataFileMeta()
    val footer = meta.footer
    assert(footer.getFileMetaData != null)
    assert(footer.getBlocks != null)
    assert(!footer.getBlocks.isEmpty)
    assert(footer.getBlocks.size() == 1)
    assert(footer.getBlocks.get(0).getRowCount == data.length)
  }
}

class NestedDataSuite extends ParquetDataFileSuite {

  private val requestStructType: StructType = new StructType()
    .add(StructField("DocId", LongType))
    .add("Links", new StructType()
      .add(StructField("Backward", ArrayType(LongType)))
      .add(StructField("Forward", ArrayType(LongType))))
    .add("Name", ArrayType(new StructType()
      .add(StructField("Language",
          ArrayType(new StructType()
          .add(StructField("Code", BinaryType))
          .add(StructField("Country", BinaryType)))))
      .add(StructField("Url", BinaryType))
      ))

  override def parquetSchema: MessageType = Paper.schema

  override def dataVersion: WriterVersion = PARQUET_2_0

  override def data: Seq[Group] = {
    val r1 = new SimpleGroup(parquetSchema)
      r1.add("DocId", 10L)
      r1.addGroup("Links")
        .append("Forward", 20L)
        .append("Forward", 40L)
        .append("Forward", 60L)
      var name = r1.addGroup("Name")
      name.addGroup("Language")
        .append("Code", "en-us")
        .append("Country", "us")
      name.addGroup("Language")
        .append("Code", "en")
      name.append("Url", "http://A")

      name = r1.addGroup("Name")
      name.append("Url", "http://B")

      name = r1.addGroup("Name")
      name.addGroup("Language")
        .append("Code", "en-gb")
        .append("Country", "gb")

      val r2 = new SimpleGroup(parquetSchema)
      r2.add("DocId", 20L)
      r2.addGroup("Links")
        .append("Backward", 10L)
        .append("Backward", 30L)
        .append("Forward", 80L)
      r2.addGroup("Name")
        .append("Url", "http://C")
    Seq(r1, r2)
  }

  test("skip read record 1") {
    val reader = ParquetDataFile(fileName, requestStructType, configuration)
    val requiredIds = Array(0, 1, 2)
    val rowIds = Array(1)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    assert(iterator.hasNext)
    val row = iterator.next
    assert(row.numFields == 3)
    val docId = row.getLong(0)
    assert(docId == 20L)
    iterator.close()
  }

  test("read all ") {
    val reader = ParquetDataFile(fileName, requestStructType, configuration)
    val requiredIds = Array(0, 2)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    assert(iterator.hasNext)
    val rowOne = iterator.next
    assert(rowOne.numFields == 2)
    val docIdOne = rowOne.getLong(0)
    assert(docIdOne == 10L)
    assert(iterator.hasNext)
    val rowTwo = iterator.next
    assert(rowTwo.numFields == 2)
    val docIdTwo = rowTwo.getLong(0)
    assert(docIdTwo == 20L)
    iterator.close()
  }
}

class VectorizedDataSuite extends ParquetDataFileSuite {

  private val requestSchema: StructType = new StructType()
    .add(StructField("int32_field", IntegerType))
    .add(StructField("int64_field", LongType))
    .add(StructField("boolean_field", BooleanType))
    .add(StructField("float_field", FloatType))

  override def parquetSchema: MessageType = new MessageType("test",
    new PrimitiveType(REQUIRED, INT32, "int32_field"),
    new PrimitiveType(REQUIRED, INT64, "int64_field"),
    new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field"),
    new PrimitiveType(REQUIRED, FLOAT, "float_field"),
    new PrimitiveType(REQUIRED, DOUBLE, "double_field")
  )

  override def dataVersion: WriterVersion = PARQUET_1_0

  override def data: Seq[Group] = {
    val factory = new SimpleGroupFactory(parquetSchema)
    (0 until 100000).map(i => factory.newGroup()
      .append("int32_field", i)
      .append("int64_field", 64L)
      .append("boolean_field", true)
      .append("float_field", 1.0f)
      .append("double_field", 2.0d))
  }

  test("read by columnIds and rowIds disable returningBatch") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = false))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0, 1)
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382, 1134, 1753, 2222, 3928, 4200, 4734)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[Int]()
    while (iterator.hasNext) {
      val row = iterator.next()
      assert(row.numFields == 2)
      result += row.getInt(0)
    }
    assert(rowIds.length == result.length)
    for (i <- rowIds.indices) {
      assert(rowIds(i) == result(i))
    }
  }

  test("read by columnIds and rowIds enable returningBatch") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = true))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0, 1)
    // RowGroup0 => page0: [0, 1, 7, 8, 120, 121, 381, 382]
    // RowGroup0 => page5: [23000]
    // RowGroup2 => page0: [50752]
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382, 23000, 50752)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[Int]()
    while (iterator.hasNext) {
      val batch = iterator.next().asInstanceOf[ColumnarBatch]
      val rowIterator = batch.rowOapIterator()
      while (rowIterator.hasNext) {
        val row = rowIterator.next()
        assert(row.numFields == 2)
        result += row.getInt(0)
      }
    }
    assert(rowIds.length == result.length)
    for (i <- rowIds.indices) {
      assert(rowIds(i) == result(i))
    }
  }

  test("read by columnIds and empty rowIds array disable returningBatch") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = false))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0, 1)
    val rowIds = Array.emptyIntArray
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    assert(!iterator.hasNext)
    val e = intercept[java.util.NoSuchElementException] {
      iterator.next()
    }.getMessage
    assert(e.contains("next on empty iterator"))
  }

  test("read by columnIds and empty rowIds array enable returningBatch") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = true))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0, 1)
    val rowIds = Array.emptyIntArray
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    assert(!iterator.hasNext)
    val e = intercept[java.util.NoSuchElementException] {
      iterator.next()
    }.getMessage
    assert(e.contains("next on empty iterator"))
  }

  test("read by columnIds disable returningBatch") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = false))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[ Int ]()
    while (iterator.hasNext) {
      val row = iterator.next()
      result += row.getInt(0)
    }
    val length = data.length
    assert(length == result.length)
    for (i <- 0 until length) {
      assert(i == result(i))
    }
  }

  test("read by columnIds enable returningBatch") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = true))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[ Int ]()
    while (iterator.hasNext) {
      val batch = iterator.next().asInstanceOf[ColumnarBatch]
      val batchIter = batch.rowIterator()
      while (batchIter.hasNext) {
        val row = batchIter.next
        result += row.getInt(0)
      }
    }
    val length = data.length
    assert(length == result.length)
    for (i <- 0 until length) {
      assert(i == result(i))
    }
  }
}

class ParquetCacheDataSuite extends ParquetDataFileSuite {

  private val requestSchema: StructType = new StructType()
    .add(StructField("int32_field", IntegerType))
    .add(StructField("int64_field", LongType))
    .add(StructField("boolean_field", BooleanType))
    .add(StructField("float_field", FloatType))

  override def parquetSchema: MessageType = new MessageType("test",
    new PrimitiveType(REQUIRED, INT32, "int32_field"),
    new PrimitiveType(REQUIRED, INT64, "int64_field"),
    new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field"),
    new PrimitiveType(REQUIRED, FLOAT, "float_field"),
    new PrimitiveType(REQUIRED, DOUBLE, "double_field")
  )

  override def dataVersion: WriterVersion = PARQUET_1_0

  override def beforeEach(): Unit = {
    super.beforeEach()
    configuration.setBoolean(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key, true)
    OapRuntime.getOrCreate.fiberCacheManager.clearAllFibers()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    configuration.unset(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key)
  }

  override def data: Seq[Group] = {
    val factory = new SimpleGroupFactory(parquetSchema)
    (0 until 100000).map(i => factory.newGroup()
      .append("int32_field", i)
      .append("int64_field", 64L)
      .append("boolean_field", true)
      .append("float_field", 1.0f)
      .append("double_field", 2.0d))
  }

  test("read by columnIds and rowIds in fiberCache") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = false))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0, 1)
    val rowIds = Array(0, 1, 7, 8, 120, 121, 381, 382, 1134, 1753, 2222, 3928, 4200, 4734)
    val iterator = reader.iteratorWithRowIds(requiredIds, rowIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[Int]()
    while (iterator.hasNext) {
      val row = iterator.next()
      assert(row.numFields == 2)
      result += row.getInt(0)
    }
    assert(rowIds.length == result.length, "Expected result length does not match.")
    for (i <- rowIds.indices) {
      assert(rowIds(i) == result(i))
    }
    assert(OapRuntime.getOrCreate.fiberCacheManager.cacheCount == 2, "Cache count does not match.")
  }

  test("read by columnIds in fiberCache") {
    val context = Some(ParquetVectorizedContext(null, null, returningBatch = false))
    val reader = ParquetDataFile(fileName, requestSchema, configuration)
    reader.setParquetVectorizedContext(context)
    val requiredIds = Array(0)
    val iterator = reader.iterator(requiredIds)
      .asInstanceOf[OapCompletionIterator[InternalRow]]
    val result = ArrayBuffer[Int]()
    while (iterator.hasNext) {
      val row = iterator.next()
      result += row.getInt(0)
    }
    val length = data.length
    assert(length == result.length, "Expected result length does not match.")
    for (i <- 0 until length) {
      assert(i == result(i))
    }
    assert(OapRuntime.getOrCreate.fiberCacheManager.cacheCount == 4, "Cache count does not match.")
  }
}

class ParquetFiberDataReaderSuite extends ParquetDataFileSuite {

  override def parquetSchema: MessageType = new MessageType("test",
    new PrimitiveType(REQUIRED, INT32, "int32_field"),
    new PrimitiveType(REQUIRED, INT64, "int64_field"),
    new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field"),
    new PrimitiveType(REQUIRED, FLOAT, "float_field"),
    new PrimitiveType(REQUIRED, DOUBLE, "double_field")
  )

  override def dataVersion: WriterVersion = PARQUET_1_0

  override def data: Seq[Group] = {
    val factory = new SimpleGroupFactory(parquetSchema)
    (0 until 1000 by 2).map(i => factory.newGroup()
      .append("int32_field", i)
      .append("int64_field", 64L)
      .append("boolean_field", true)
      .append("float_field", 1.0f)
      .append("double_field", 2.0d))
  }

  test("read single column in a group") {
    val meta = ParquetDataFileMeta(configuration, fileName)
    val reader = ParquetFiberDataReader.open(configuration,
      new Path(fileName), meta.footer.toParquetMetadata)
    val footer = reader.getFooter
    val rowCount = footer.getBlocks.get(0).getRowCount.toInt
    val vector = ColumnVectorAdapter.allocate(rowCount, IntegerType, MemoryMode.ON_HEAP)
    val blockMetaData = footer.getBlocks.get(0)
    val columnDescriptor = parquetSchema.getColumns.get(0)
    val types: util.List[Type] = parquetSchema.asGroupType.getFields
    val fiberData = reader.readFiberData(blockMetaData, columnDescriptor)
    val columnReader =
      new VectorizedColumnReaderWrapper(
        new VectorizedColumnReader(columnDescriptor, types.get(0).getOriginalType,
          fiberData.getPageReader(columnDescriptor), TimeZone.getDefault))
    columnReader.readBatch(rowCount, vector)
    for (i <- 0 until rowCount) {
      assert(i * 2 == vector.getInt(i))
    }
  }

  test("can not find column meta") {
    val meta = ParquetDataFileMeta(configuration, fileName)
    val reader = ParquetFiberDataReader.open(configuration,
      new Path(fileName), meta.footer.toParquetMetadata)
    val footer = reader.getFooter
    val rowCount = footer.getBlocks.get(0).getRowCount.toInt
    val vector = ColumnVectorAdapter.allocate(rowCount, IntegerType, MemoryMode.ON_HEAP)
    val blockMetaData = footer.getBlocks.get(0)
    val columnDescriptor = new ColumnDescriptor(Array(s"${fileName}_temp"), INT32, 0, 0)
    val exception = intercept[IOException] {
      reader.readFiberData(blockMetaData, columnDescriptor)
    }
    assert(exception.getMessage.contains("Can not find column meta of column"))
  }
}

class ParquetFiberDataLoaderSuite extends ParquetDataFileSuite {

  private val requestSchema: StructType = new StructType()
    .add(StructField("int32_field", IntegerType))
    .add(StructField("int64_field", LongType))
    .add(StructField("boolean_field", BooleanType))
    .add(StructField("float_field", FloatType))
    .add(StructField("string_field", StringType))

  override def parquetSchema: MessageType = new MessageType("test",
    new PrimitiveType(REQUIRED, INT32, "int32_field"),
    new PrimitiveType(REQUIRED, INT64, "int64_field"),
    new PrimitiveType(REQUIRED, BOOLEAN, "boolean_field"),
    new PrimitiveType(REQUIRED, FLOAT, "float_field"),
    new PrimitiveType(REQUIRED, BINARY, "string_field")
  )

  override def dataVersion: WriterVersion = PARQUET_1_0

  override def data: Seq[Group] = {
    val factory = new SimpleGroupFactory(parquetSchema)
    (0 until 100).map(i => factory.newGroup()
      .append("int32_field", i)
      .append("int64_field", 64L)
      .append("boolean_field", true)
      .append("float_field", 1.0f)
      .append("string_field", s"str$i"))
  }

  var reader: ParquetFiberDataReader = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    reader = ParquetFiberDataReader.open(configuration, new Path(fileName),
      ParquetDataFileMeta(configuration, fileName).footer.toParquetMetadata)
  }

  override def afterEach(): Unit = {
    reader.close()
    super.afterEach()
  }

  private def addRequestSchemaToConf(conf: Configuration, requiredIds: Array[Int]): Unit = {
    val requestSchemaString = {
      var schema = new StructType
      for (index <- requiredIds) {
        schema = schema.add(requestSchema(index))
      }
      schema.json
    }
    conf.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaString)
  }

  private def loadSingleColumn(requiredId: Array[Int]): FiberCache = {
    val conf = new Configuration(configuration)
    addRequestSchemaToConf(conf, requiredId)
    ParquetFiberDataLoader(conf, reader, 0).loadSingleColumn
  }

  test("test loadSingleColumn with reuse reader") {
    // fixed length data type
    val rowCount = reader.getFooter.getBlocks.get(0).getRowCount.toInt
    val intFiberCache = loadSingleColumn(Array(0))
    (0 until rowCount).foreach(i => assert(intFiberCache.getInt(i * 4) == i))
    // variable length data type
    val strFiberCache = loadSingleColumn(Array(4))
    (0 until rowCount).map { i =>
      val length = strFiberCache.getInt(i * 4)
      val offset = strFiberCache.getInt(rowCount * 4 + i * 4)
      assert(strFiberCache.getUTF8String(rowCount * 9 + offset, length).
        equals(UTF8String.fromString(s"str$i")))
    }
  }

  test("test load multi-columns every time") {
    val exception = intercept[IllegalArgumentException] {
      loadSingleColumn(Array(0, 1))
    }
    assert(exception.getMessage.contains("Only can get single column every time"))
  }
}

