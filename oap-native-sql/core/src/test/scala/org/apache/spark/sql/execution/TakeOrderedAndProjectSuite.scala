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

package org.apache.spark.sql.execution

import org.apache.spark.SparkConf

import scala.util.Random
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._


class TakeOrderedAndProjectSuite extends SparkPlanTest with SharedSparkSession {

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
      //.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "10m")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .set("spark.sql.columnar.codegen.hashAggregate", "false")
      .set("spark.oap.sql.columnar.wholestagecodegen", "false")
      .set("spark.sql.columnar.window", "false")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      //.set("spark.sql.columnar.tmp_dir", "/codegen/nativesql/")
      .set("spark.sql.columnar.sort.broadcastJoin", "true")
      .set("spark.oap.sql.columnar.preferColumnar", "true")

  private var rand: Random = _
  private var seed: Long = 0

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    seed = System.currentTimeMillis()
    rand = new Random(seed)
  }

  private def generateRandomInputData(): DataFrame = {
    val schema = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", IntegerType, nullable = false)
    val inputData = Seq.fill(10000)(Row(rand.nextInt(), rand.nextInt()))
    spark.createDataFrame(sparkContext.parallelize(Random.shuffle(inputData), 10), schema)
  }

  /**
   * Adds a no-op filter to the child plan in order to prevent executeCollect() from being
   * called directly on the child plan.
   */
  private def noOpFilter(plan: SparkPlan): SparkPlan = FilterExec(Literal(true), plan)

  val limit = 250
  val sortOrder = 'a.desc :: 'b.desc :: Nil

  ignore("TakeOrderedAndProject.doExecute without project") {
    withClue(s"seed = $seed") {
      checkThatPlansAgree(
        generateRandomInputData(),
        input =>
          noOpFilter(TakeOrderedAndProjectExec(limit, sortOrder, input.output, input)),
        input =>
          GlobalLimitExec(limit,
            LocalLimitExec(limit,
              SortExec(sortOrder, true, input))),
        sortAnswers = false)
    }
  }

  ignore("TakeOrderedAndProject.doExecute with project") {
    withClue(s"seed = $seed") {
      checkThatPlansAgree(
        generateRandomInputData(),
        input =>
          noOpFilter(
            TakeOrderedAndProjectExec(limit, sortOrder, Seq(input.output.last), input)),
        input =>
          GlobalLimitExec(limit,
            LocalLimitExec(limit,
              ProjectExec(Seq(input.output.last),
                SortExec(sortOrder, true, input)))),
        sortAnswers = false)
    }
  }
}
