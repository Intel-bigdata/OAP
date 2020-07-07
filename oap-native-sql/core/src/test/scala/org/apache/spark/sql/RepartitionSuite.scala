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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{
  ColumnarShuffleExchangeExec,
  ColumnarToRowExec,
  RowToColumnarExec
}
import org.apache.spark.sql.test.SharedSparkSession

class RepartitionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test repartition")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  def checkCoulumnarExec(data: DataFrame) = {
    val found = data.queryExecution.executedPlan
      .collect {
        case r2c: RowToColumnarExec => 1
        case c2r: ColumnarToRowExec => 10
        case exc: ColumnarShuffleExchangeExec => 100
      }
      .distinct
      .sum
    assert(found == 110)
  }

  def withInput(input: DataFrame)(
      transformation: Option[DataFrame => DataFrame],
      repartition: DataFrame => DataFrame): Unit = {
    val expected = transformation.getOrElse(identity[DataFrame](_))(input)
    val data = repartition(expected)
    checkCoulumnarExec(data)
    checkAnswer(data, expected)
  }

  lazy val input: DataFrame = Seq((1, "1"), (2, "20"), (3, "300")).toDF("id", "val")

  def withTransformationAndRepartition(
      transformation: DataFrame => DataFrame,
      repartition: DataFrame => DataFrame): Unit =
    withInput(input)(Some(transformation), repartition)

  def withRepartition: (DataFrame => DataFrame) => Unit = withInput(input)(None, _)
}

class SmallDataRepartitionSuite extends RepartitionSuite {
  import testImplicits._

  test("test round robin partitioning") {
    withRepartition(df => df.repartition(2))
  }

  test("test hash partitioning") {
    withRepartition(df => df.repartition('id))
  }

  test("test range partitioning") {
    withRepartition(df => df.repartitionByRange('id))
  }

  ignore("test cached repartiiton") {
    val data = input.cache.repartition(2)

    val found = data.queryExecution.executedPlan.collect {
      case cache: InMemoryTableScanExec => 1
      case c2r: ColumnarToRowExec => 10
      case exc: ColumnarShuffleExchangeExec => 100
    }.sum
    assert(found == 111)

    checkAnswer(data, input)
  }
}

class TPCHTableRepartitionSuite extends RepartitionSuite {
  import testImplicits._

  val filePath = getClass.getClassLoader
    .getResource("part-00000-d648dd34-c9d2-4fe9-87f2-770ef3551442-c000.snappy.parquet")
    .getFile

  override lazy val input = spark.read.parquet(filePath)

  test("test tpch round robin partitioning") {
    withRepartition(df => df.repartition(2))
  }

  test("test tpch hash partitioning") {
    withRepartition(df => df.repartition('n_nationkey))
  }

  test("test tpch range partitioning") {
    withRepartition(df => df.repartitionByRange('n_name))
  }

  test("test tpch sum after repartition") {
    withTransformationAndRepartition(
      df => df.groupBy("n_regionkey").agg(Map("n_nationkey" -> "sum")),
      df => df.repartition(2))
  }
}

class DisableColumnarShuffle extends SmallDataRepartitionSuite {
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.sql.codegen.wholeStage", "true")
  }

  override def checkCoulumnarExec(data: DataFrame) = {
    val found = data.queryExecution.executedPlan
      .collect {
        case c2r: ColumnarToRowExec => 1
        case exc: ColumnarShuffleExchangeExec => 10
        case exc: ShuffleExchangeExec => 100
      }
      .distinct
      .sum
    assert(found == 101)
  }
}
