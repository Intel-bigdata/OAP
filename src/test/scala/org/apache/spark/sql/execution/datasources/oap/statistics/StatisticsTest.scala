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

package org.apache.spark.sql.execution.datasources.oap.statistics

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BaseOrdering, GenerateOrdering}
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index.RangeInterval
import org.apache.spark.sql.execution.datasources.oap.utils.{NonNullKeyReader, NonNullKeyWriter}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.unsafe.types.UTF8String

abstract class StatisticsTest extends SparkFunSuite with BeforeAndAfterEach {

  protected def rowGen(i: Int): InternalRow = InternalRow(i, UTF8String.fromString(s"test#$i"))

  protected lazy val schema: StructType = StructType(StructField("a", IntegerType)
    :: StructField("b", StringType) :: Nil)
  @transient
  protected lazy val nnkw: NonNullKeyWriter = new NonNullKeyWriter(schema)
  @transient
  protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(schema)
  @transient
  protected lazy val ordering: BaseOrdering = GenerateOrdering.create(schema)
  @transient
  protected lazy val partialOrdering: BaseOrdering =
    GenerateOrdering.create(StructType(schema.dropRight(1)))
  protected var out: ByteArrayOutputStream = _

  protected var intervalArray: ArrayBuffer[RangeInterval] = new ArrayBuffer[RangeInterval]()

  override def beforeEach(): Unit = {
    out = new ByteArrayOutputStream(8000)
  }

  override def afterEach(): Unit = {
    out.close()
    intervalArray.clear()
  }

  protected def generateInterval(
      start: InternalRow, end: InternalRow,
      startInclude: Boolean, endInclude: Boolean): Unit = {
    intervalArray.clear()
    intervalArray.append(new RangeInterval(start, end, startInclude, endInclude))
  }

  protected def checkInternalRow(row1: InternalRow, row2: InternalRow): Unit = {
    val res = row1 == row2 // it works..
    assert(res, s"row1: $row1 does not match $row2")
  }

  protected def wrapToFiberCache(out: ByteArrayOutputStream): FiberCache =
    new FiberCache {
      val bytes = out.toByteArray
      override protected def fiberData: MemoryBlock =
        new MemoryBlock(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length)
    }
}
