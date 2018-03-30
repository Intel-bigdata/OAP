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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Now We don't have mock utils, so OapUtilsSuite extends SharedOapContext to use SparkSession.
 */
class OapUtilsSuite extends SharedOapContext {

  test("test rootPaths empty") {
    val fileIndex = new InMemoryFileIndex(spark, Seq.empty, Map.empty, None)
    intercept[AssertionError] {
      OapUtils.getOutPutPath(fileIndex)
    }
  }

  test("test rootPaths length eq 1 no partitioned") {
    val tablePath = new Path("/table")
    val rootPaths = Seq(tablePath)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, None)
    val ret = OapUtils.getOutPutPath(fileIndex)
    assert(ret.equals(tablePath))
  }

  test("test rootPaths length eq 1 partitioned") {
    val tablePath = new Path("/table")
    val partitionSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StringType))
    val rootPaths = Seq(tablePath)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, Some(partitionSchema))
    val ret = OapUtils.getOutPutPath(fileIndex)
    assert(ret.equals(tablePath))
  }

  test("test rootPaths length more than 1") {
    val part1 = new Path("/table/a=1/b=1")
    val part2 = new Path("/table/a=1/b=2")
    val tablePath = new Path("/table")
    val partitionSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StringType))
    val rootPaths = Seq(part1, part2)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, Some(partitionSchema))
    val ret = OapUtils.getOutPutPath(fileIndex)
    assert(ret.equals(tablePath))
  }

  test("test rootPaths length eq 1 but partitioned") {
    val part1 = new Path("/table/a=1/b=1")
    val partitionSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StringType))
    val rootPaths = Seq(part1)
    val fileIndex = new InMemoryFileIndex(spark, rootPaths, Map.empty, Some(partitionSchema))
    val ret = OapUtils.getOutPutPath(fileIndex)
    assert(ret.equals(part1))
  }
}
