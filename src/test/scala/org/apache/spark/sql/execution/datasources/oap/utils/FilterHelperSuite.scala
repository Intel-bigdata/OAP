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

package org.apache.spark.sql.execution.datasources.oap.utils

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetInputFormat

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class FilterHelperSuite extends SparkFunSuite {
  val conf = SQLConf.get

  test("Pushed And Set") {
    val requiredSchema = new StructType()
      .add(StructField("a", IntegerType))
      .add(StructField("b", StringType))
    val filters = Seq(GreaterThan("a", 1), EqualTo("b", "2"))
    val expected = s"""and(gt(a, 1), eq(b, Binary{"2"}))"""
    conf.setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED, true)
    val pushed = FilterHelper.tryToPushFilters(conf, requiredSchema, filters)
    assert(pushed.isDefined)
    assert(pushed.get.toString.equals(expected))
    val config = new Configuration()
    FilterHelper.setFilterIfExist(config, pushed)
    val humanReadable = config.get(ParquetInputFormat.FILTER_PREDICATE + ".human.readable")
    assert(humanReadable.nonEmpty)
    assert(humanReadable.equals(expected))
  }

  test("Not Pushed") {
    val requiredSchema = new StructType()
      .add(StructField("a", IntegerType))
      .add(StructField("b", StringType))
    val filters = Seq(GreaterThan("a", 1), EqualTo("b", "2"))
    conf.setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED, false)
    val pushed = FilterHelper.tryToPushFilters(conf, requiredSchema, filters)
    assert(pushed.isEmpty)
    val config = new Configuration()
    FilterHelper.setFilterIfExist(config, pushed)
    assert(config.get(ParquetInputFormat.FILTER_PREDICATE) == null)
    assert(config.get(ParquetInputFormat.FILTER_PREDICATE + ".human.readable") == null)
  }
}
