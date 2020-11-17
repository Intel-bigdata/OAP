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

package com.intel.oap

import org.apache.spark.SparkConf

class ColumnarPluginConfig(conf: SparkConf) {
  val enableColumnarSort: Boolean =
    conf.getBoolean("spark.sql.columnar.sort", defaultValue = false)
  val enableCodegenHashAggregate: Boolean =
    conf.getBoolean("spark.sql.columnar.codegen.hashAggregate", defaultValue = false)
  val enableColumnarBroadcastJoin: Boolean =
    conf.getBoolean("spark.sql.columnar.sort.broadcastJoin", defaultValue = true)
  val enableColumnarWindow: Boolean =
    conf.getBoolean("spark.sql.columnar.window", defaultValue = true)
  val enableColumnarSortMergeJoin: Boolean =
    conf.getBoolean("spark.oap.sql.columnar.sortmergejoin", defaultValue = false)
  val enablePreferColumnar: Boolean =
    conf.getBoolean("spark.oap.sql.columnar.preferColumnar", defaultValue = false)
  val enableJoinOptimizationReplace: Boolean =
    conf.getBoolean("spark.oap.sql.columnar.joinOptimizationReplace", defaultValue = false)
  val joinOptimizationThrottle: Integer =
    conf.getInt("spark.oap.sql.columnar.joinOptimizationLevel", defaultValue = 6)
  val enableColumnarWholeStageCodegen: Boolean =
    conf.getBoolean("spark.oap.sql.columnar.wholestagecodegen", defaultValue = true)
  val enableColumnarShuffle: Boolean = conf
    .get("spark.shuffle.manager", "sort")
    .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  val batchSize: Int =
    conf.getInt("spark.sql.execution.arrow.maxRecordsPerBatch", defaultValue = 10000)
  val tmpFile: String =
    conf.getOption("spark.sql.columnar.tmp_dir").getOrElse(null)
  val broadcastCacheTimeout: Int =
    conf.getInt("spark.sql.columnar.sort.broadcast.cache.timeout", defaultValue = -1)
}

object ColumnarPluginConfig {
  var ins: ColumnarPluginConfig = null
  var random_temp_dir_path: String = null
  def getConf(conf: SparkConf): ColumnarPluginConfig = synchronized {
    if (ins == null) {
      ins = new ColumnarPluginConfig(conf)
      ins
    } else {
      ins
    }
  }
  def getConf: ColumnarPluginConfig = synchronized {
    if (ins == null) {
      throw new IllegalStateException("ColumnarPluginConfig is not initialized yet")
    } else {
      ins
    }
  }
  def getBatchSize: Int = synchronized {
    if (ins == null) {
      10000
    } else {
      ins.batchSize
    }
  }
  def getTempFile: String = synchronized {
    if (ins != null && ins.tmpFile != null) {
      ins.tmpFile
    } else {
      System.getProperty("java.io.tmpdir")
    }
  }
  def setRandomTempDir(path: String) = synchronized {
    random_temp_dir_path = path
  }
  def getRandomTempDir = synchronized {
    random_temp_dir_path
  }
}
