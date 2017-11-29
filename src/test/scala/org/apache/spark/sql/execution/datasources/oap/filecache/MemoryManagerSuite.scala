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

package org.apache.spark.sql.execution.datasources.oap.filecache

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.parquet.bytes.LittleEndianDataOutputStream

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.OapEnv
import org.apache.spark.sql.execution.datasources.oap.index.IndexUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ByteBufferOutputStream, Utils}

class MemoryManagerSuite extends SharedSQLContext {
  sparkConf
      .set("spark.memory.offHeap.size", "100m")
      .set("spark.oap.memory.offHeap.fraction", "0.7")

  private var fiberCache: FiberCache = _
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    fiberCache = {
      val buf = new ByteBufferOutputStream()
      val writer = new LittleEndianDataOutputStream(buf)
      values.foreach(value => IndexUtils.writeBasedOnDataType(writer, value))
      memoryManager.putToDataFiberCache(buf.toByteArray)
    }
  }

  private def memoryManager = OapEnv.get.memoryManager

  private val random = new Random(0)
  private val values = {
    val booleans: Seq[Boolean] = Seq(true, false)
    val bytes: Seq[Byte] = Seq(Byte.MinValue, 0, 10, 30, Byte.MaxValue)
    val shorts: Seq[Short] = Seq(Short.MinValue, -100, 0, 10, 200, Short.MaxValue)
    val ints: Seq[Int] = Seq(Int.MinValue, -100, 0, 100, 12346, Int.MaxValue)
    val longs: Seq[Long] = Seq(Long.MinValue, -10000, 0, 20, Long.MaxValue)
    val floats: Seq[Float] = Seq(Float.MinValue, Float.MinPositiveValue, Float.MaxValue)
    val doubles: Seq[Double] = Seq(Double.MinValue, Double.MinPositiveValue, Double.MaxValue)
    val strings: Seq[UTF8String] =
      Seq("", "test", "b plus tree", "MemoryManagerSuite").map(UTF8String.fromString)
    val binaries: Seq[Array[Byte]] = (0 until 20 by 5).map{ size =>
      val buf = new Array[Byte](size)
      random.nextBytes(buf)
      buf
    }
    val values = booleans ++ bytes ++ shorts ++ ints ++ longs ++
        floats ++ doubles ++ strings ++ binaries ++ Nil
    random.shuffle(values)
  }

  // TODO: This will throw NPE with mvn test
  test("test MemoryManager config") {
    // default fraction is 0.7
    assert(100 * 0.7 * 1024 * 1024 === memoryManager.maxMemory)
    // change to 0.9
    sparkContext.conf.set(MemoryManager.OAP_OFF_HEAP_MEMORY_FRACTION, "0.9")
    OapEnv.update()
    assert(100 * 0.9 * 1024 * 1024 === memoryManager.maxMemory)
    // restore back
    sparkContext.conf.set(
      MemoryManager.OAP_OFF_HEAP_MEMORY_FRACTION,
      MemoryManager.OAP_OFF_HEAP_MEMORY_FRACTION_DEFAULT.toString)
    OapEnv.update()
    assert(100 * 0.7 * 1024 * 1024 === memoryManager.maxMemory)
  }

  test("test data in IndexFiberCache") {
    // TODO: find a nice way to create FSDataInputStream
    def createInputStreamFromBytes(bytes: Array[Byte]): FSDataInputStream = {
      val tempDir = Utils.createTempDir().getAbsolutePath
      val fileName = new Path(tempDir, "temp")
      val fs = fileName.getFileSystem(new Configuration())
      val writer = fs.create(fileName)
      writer.write(bytes)
      writer.close()
      fs.open(fileName)
    }
    val bytes = new Array[Byte](10240)
    random.nextBytes(bytes)
    val is = createInputStreamFromBytes(bytes)
    val indexFiberCache = memoryManager.putToIndexFiberCache(is, 0, 10240)
    assert(bytes === indexFiberCache.toArray)
  }

  test("test data in DataFiberCache") {
    val bytes = new Array[Byte](10240)
    random.nextBytes(bytes)
    val dataFiberCache = memoryManager.putToDataFiberCache(bytes)
    assert(bytes === dataFiberCache.toArray)
  }

  test("test FiberCache readInt") {
    var offset = 0
    values.foreach {
      case bool: Boolean =>
        assert(fiberCache.getBoolean(offset) === bool)
        offset += BooleanType.defaultSize
      case short: Short =>
        assert(fiberCache.getShort(offset) === short)
        offset += ShortType.defaultSize
      case byte: Byte =>
        assert(fiberCache.getByte(offset) === byte)
        offset += ByteType.defaultSize
      case int: Int =>
        assert(fiberCache.getInt(offset) === int)
        offset += IntegerType.defaultSize
      case long: Long =>
        assert(fiberCache.getLong(offset) === long)
        offset += LongType.defaultSize
      case float: Float =>
        assert(fiberCache.getFloat(offset) === float)
        offset += FloatType.defaultSize
      case double: Double =>
        assert(fiberCache.getDouble(offset) === double)
        offset += DoubleType.defaultSize
      case string: UTF8String =>
        val length = fiberCache.getInt(offset)
        assert(length === string.numBytes())
        offset += IntegerType.defaultSize
        assert(fiberCache.getUTF8String(offset, length) === string)
        offset += length
      case bytes: Array[Byte] =>
        val length = fiberCache.getInt(offset)
        assert(length === bytes.length)
        offset += IntegerType.defaultSize
        assert(fiberCache.getBytes(offset, length) === bytes)
        offset += length
    }
  }

  test("check invalidate FiberCache") {
    // 1. disposed FiberCache
    val bytes = new Array[Byte](1024)
    val fiberCache = memoryManager.putToDataFiberCache(bytes)
    fiberCache.dispose()
    val exception = intercept[OapException]{
      fiberCache.getByte(0)
    }
    assert(exception.getMessage == "Try to access a freed memory")

    // 2. TODO: test Invalidate MemoryBlock
  }
}
