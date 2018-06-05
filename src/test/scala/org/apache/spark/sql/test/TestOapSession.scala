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

package org.apache.spark.sql.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.DebugFilesystem
import org.apache.spark.sql.SparkSession

/**
 * A special [[SparkSession]] prepared for OAP testing.
 */
private[sql] class TestOapSession(sc: SparkContext) extends TestSparkSession(sc) { self =>

  def this(sparkConf: SparkConf) {
    this(new SparkContext(
      "local[2]",
      "test-oap-context",
      sparkConf.set("spark.sql.testkey", "true")
        .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)))
  }
}

/**
 * A special [[SparkSession]] prepared for OAP testing in LocalClusterMode. This session is used by
 * extending [[org.apache.spark.sql.test.oap.SharedOapLocalClusterContext]]
 */
private[sql] class TestOapLocalClusterSession(sc: SparkContext) extends TestSparkSession(sc) {
  self =>
  def this(sparkConf: SparkConf) {
    this(new SparkContext(
      s"local-cluster[2, 2, 1024]",
      "test-oap-local-cluster-context",
      sparkConf.set("spark.sql.testkey", "true")
        .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)))
  }
}
