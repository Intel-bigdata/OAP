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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.adaptive.OptimizeLocalShuffleReader.LOCAL_SHUFFLE_READER_DESCRIPTION
import org.apache.spark.sql.execution.{PartialReducerPartitionSpec, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ColumnarAdaptiveQueryExecSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  setupTestData()

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test columnar aqe")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "10m")
      .set("spark.oap.sql.columnar.preferColumnar", "true")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")

  private def runAdaptiveAndVerifyResult(query: String): (SparkPlan, SparkPlan) = {
    var finalPlanCnt = 0
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case SparkListenerSQLAdaptiveExecutionUpdate(_, _, sparkPlanInfo) =>
            if (sparkPlanInfo.simpleString.startsWith("AdaptiveSparkPlan isFinalPlan=true")) {
              finalPlanCnt += 1
            }
          case _ => // ignore other events
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)

    val dfAdaptive = sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))
    val result = dfAdaptive.collect()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      QueryTest.sameRows(result.toSeq, df.collect().toSeq)
    }
    val planAfter = dfAdaptive.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    spark.sparkContext.listenerBus.waitUntilEmpty()
    // AQE will post `SparkListenerSQLAdaptiveExecutionUpdate` twice in case of subqueries that
    // exist out of query stages.
    val expectedFinalPlanCnt = adaptivePlan.find(_.subqueries.nonEmpty).map(_ => 2).getOrElse(1)
    assert(finalPlanCnt == expectedFinalPlanCnt)
    spark.sparkContext.removeSparkListener(listener)

    val exchanges = adaptivePlan.collect {
      case e: Exchange => e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def findTopLevelBroadcastHashJoin(plan: SparkPlan): Seq[BroadcastHashJoinExec] = {
    collect(plan) {
      case j: BroadcastHashJoinExec => j
    }
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] = {
    collect(plan) {
      case j: SortMergeJoinExec => j
    }
  }

  private def findReusedExchange(plan: SparkPlan): Seq[ReusedExchangeExec] = {
    collectWithSubqueries(plan) {
      case ShuffleQueryStageExec(_, e: ReusedExchangeExec) => e
      case BroadcastQueryStageExec(_, e: ReusedExchangeExec) => e
    }
  }

  private def findReusedSubquery(plan: SparkPlan): Seq[ReusedSubqueryExec] = {
    collectWithSubqueries(plan) {
      case e: ReusedSubqueryExec => e
    }
  }

  private def checkNumLocalShuffleReaders(
      plan: SparkPlan,
      numShufflesWithoutLocalReader: Int = 0): Unit = {
    val numShuffles = collect(plan) {
      case s: ShuffleQueryStageExec => s
    }.length

    val numLocalReaders = collect(plan) {
      case reader @ CustomShuffleReaderExec(_, _, LOCAL_SHUFFLE_READER_DESCRIPTION) => reader
    }
    numLocalReaders.foreach { r =>
      val rdd = r.execute()
      val parts = rdd.partitions
      assert(parts.forall(rdd.preferredLocations(_).nonEmpty))
    }
    assert(numShuffles === (numLocalReaders.length + numShufflesWithoutLocalReader))
  }

  ignore("Columnar exchange reuse") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT value FROM testData join testData2 ON key = a " +
          "join (SELECT value v from testData join testData3 ON key = a) on value = v")
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.size == 1)
    }
  }

  ignore("SPARK-29544: adaptive skew join with different join types") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "100",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "800",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "800",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR.key -> "2") {
      withTempView("skewData1", "skewData2") {
        spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 250, 249)
              .when('id >= 750, 1000)
              .otherwise('id)
              .as("key1"),
            'id as "value1")
          .createOrReplaceTempView("skewData1")
        spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 250, 249)
              .otherwise('id)
              .as("key2"),
            'id as "value2")
          .createOrReplaceTempView("skewData2")

        def checkSkewJoin(
            joins: Seq[SortMergeJoinExec],
            leftSkewNum: Int,
            rightSkewNum: Int): Unit = {
          assert(joins.size == 1 && joins.head.isSkewJoin)
          assert(
            joins.head.left
              .collect {
                case r: ColumnarCustomShuffleReaderExec => r
              }
              .head
              .partitionSpecs
              .collect {
                case p: PartialReducerPartitionSpec => p.reducerIndex
              }
              .distinct
              .length == leftSkewNum)
          assert(
            joins.head.right
              .collect {
                case r: ColumnarCustomShuffleReaderExec => r
              }
              .head
              .partitionSpecs
              .collect {
                case p: PartialReducerPartitionSpec => p.reducerIndex
              }
              .distinct
              .length == rightSkewNum)
        }

        // skewed inner join optimization
        val (_, innerAdaptivePlan) =
          runAdaptiveAndVerifyResult("SELECT * FROM skewData1 join skewData2 ON key1 = key2")
        val innerSmj = findTopLevelSortMergeJoin(innerAdaptivePlan)
        checkSkewJoin(innerSmj, 1, 1)

        // skewed left outer join optimization
        val (_, leftAdaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM skewData1 left outer join skewData2 ON key1 = key2")
        val leftSmj = findTopLevelSortMergeJoin(leftAdaptivePlan)
        checkSkewJoin(leftSmj, 1, 0)

        // skewed right outer join optimization
        val (_, rightAdaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM skewData1 right outer join skewData2 ON key1 = key2")
        val rightSmj = findTopLevelSortMergeJoin(rightAdaptivePlan)
        checkSkewJoin(rightSmj, 0, 1)
      }
    }
  }
}
