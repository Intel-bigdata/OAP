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

package org.apache.spark.sql.execution.datasources.oap

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.{OapSession, SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.sql.hive.OapSessionState
import org.apache.spark.sql.internal.SQLConf

object TestOap
  extends TestOapContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[2]"),
      "TestOapContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set("spark.sql.testkey", "true")
        .set(CATALOG_IMPLEMENTATION, "hive")
        .set("spark.memory.offHeap.size", "100m")))

private[oap] class TestOapContext(
    @transient override val sparkSession: TestOapSparkSession)
  extends SQLContext(sparkSession) {

  implicit def sqlContext: SQLContext = spark.sqlContext
  implicit def spark: SparkSession = sparkSession

  object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  def this(sc: SparkContext) {
    this(new TestOapSparkSession(sc))
  }
}

private[oap] object TestOapContext {

  /**
    * A map used to store all confs that need to be overridden in oap unit tests.
    */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.OAP_IS_TESTING.key -> "true")
}

private[oap] class TestOapSparkSession(
    @transient private val sc: SparkContext)
  extends OapSession(sc) with Logging { self =>

  assume(sc.conf.get(CATALOG_IMPLEMENTATION) == "hive")

  @transient
  override lazy val sessionState: TestOapSessionState =
    new TestOapSessionState(self)
}

private[oap] class TestOapSessionState(sparkSession: TestOapSparkSession)
  extends OapSessionState(sparkSession) { self =>

  override lazy val conf: SQLConf = {
    new SQLConf {
      clear()
      override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)
      override def clear(): Unit = {
        super.clear()
        TestOapContext.overrideConfs.foreach { case (k, v) => setConfString(k, v) }
      }
    }
  }
}
