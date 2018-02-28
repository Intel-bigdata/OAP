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
import org.apache.spark.sql.internal.oap.OapConf

object TestOap extends TestOapContext(
  OapSession.builder.config(
    (new SparkConf).set("spark.master", "local[2]")
      .set("spark.app.name", "test-oap-context")
      .set("spark.sql.testkey", "true")
      .set("spark.memory.offHeap.size", "100m")
  ).enableHiveSupport().getOrCreate()) {
}

/**
 * A locally running test instance of Oap engine.
 */
class TestOapContext(
    @transient override val sparkSession: SparkSession)
  extends SQLContext(sparkSession) {

  protected def sqlContext: SQLContext = sparkSession.sqlContext

  // OapStrategy conflicts with EXECUTOR_INDEX_SELECTION.
  sqlContext.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key, "false")
}
