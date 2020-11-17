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

import com.intel.oap.execution.ColumnarHashAggregateExec
import com.intel.oap.datasource.parquet.ParquetReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
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
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "10m")

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

class TPCHTableRepartitionSuite extends RepartitionSuite {
  import testImplicits._

  val filePath = getTestResourcePath(
    "test-data/part-00000-d648dd34-c9d2-4fe9-87f2-770ef3551442-c000.snappy.parquet")

  override lazy val input = spark.read.format("arrow").load(filePath)

  ignore("tpch table round robin partitioning") {
    withRepartition(df => df.repartition(2))
  }

  ignore("tpch table hash partitioning") {
    withRepartition(df => df.repartition('n_nationkey))
  }

  ignore("tpch table range partitioning") {
    withRepartition(df => df.repartitionByRange('n_name))
  }

  ignore("tpch table hash partitioning with expression") {
    withRepartition(df => df.repartition('n_nationkey + 'n_regionkey))
  }

  ignore("tpch table sum after repartition") {
    withTransformationAndRepartition(
      df => df.groupBy("n_regionkey").agg(Map("n_nationkey" -> "sum")),
      df => df.repartition(2))
  }
}

class DisableColumnarShuffleSuite extends RepartitionSuite {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.sql.codegen.wholeStage", "true")
  }

  override def checkCoulumnarExec(data: DataFrame) = {
    val found = data.queryExecution.executedPlan
      .collectFirst {
        case exc: ColumnarShuffleExchangeExec => exc
      }
    data.explain
    assert(found.isEmpty)
  }

  test("round robin partitioning") {
    withRepartition(df => df.repartition(2))
  }

  test("hash partitioning") {
    withRepartition(df => df.repartition('id))
  }

  test("range partitioning") {
    withRepartition(df => df.repartitionByRange('id))
  }
}

class AdaptiveQueryExecRepartitionSuite extends TPCHTableRepartitionSuite {
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "true")
  }

  def checkBefore(data: DataFrame) = {
    val planBefore = data.queryExecution.executedPlan
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))
  }

  def checkAfter(data: DataFrame) = {
    val planAfter = data.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    val found = adaptivePlan
      .collect {
        case c2r: ColumnarToRowExec => 1
        case row: ShuffleExchangeExec => 10
        case col: ColumnarShuffleExchangeExec => 100
      }
      .distinct
      .sum
    assert(found == 1, "The final plan should not contain any Exchange node.")
  }

  override def withInput(input: DataFrame)(
      transformation: Option[DataFrame => DataFrame],
      repartition: DataFrame => DataFrame): Unit = {
    val expected = transformation.getOrElse(identity[DataFrame](_))(input)
    val data = repartition(expected)
    checkBefore(data)
    checkAnswer(data, expected)
    checkAfter(data)
  }

}

class ReuseExchangeSuite extends RepartitionSuite {
  val filePath = getTestResourcePath(
    "test-data/part-00000-d648dd34-c9d2-4fe9-87f2-770ef3551442-c000.snappy.parquet")

  override lazy val input = spark.read.parquet(filePath)

  test("columnar exchange same result") {
    val df1 = input.groupBy("n_regionkey").agg(Map("n_nationkey" -> "sum"))
    val hashAgg1 = df1.queryExecution.executedPlan.collectFirst {
      case agg: ColumnarHashAggregateExec => agg
    }.get

    val df2 = input.groupBy("n_regionkey").agg(Map("n_nationkey" -> "sum"))
    val hashAgg2 = df2.queryExecution.executedPlan.collectFirst {
      case agg: ColumnarHashAggregateExec => agg
    }.get

    assert(hashAgg1.sameResult(hashAgg2))

    val exchange1 = new ColumnarShuffleExchangeExec(UnknownPartitioning(1), hashAgg1)
    val exchange2 = new ColumnarShuffleExchangeExec(UnknownPartitioning(1), hashAgg2)
    assert(exchange1.sameResult(exchange2))
  }
}
