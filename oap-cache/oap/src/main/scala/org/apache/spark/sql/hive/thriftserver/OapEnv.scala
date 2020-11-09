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

package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.oap.listener.OapListener
import org.apache.spark.sql.oap.ui.OapTab
import org.apache.spark.util.Utils

/**
 * Most of the code in init() are copied from SparkSQLEnv. Please include code from the
 * corresponding Spark version.
 */
private[spark] object OapEnv extends Logging {
  logDebug("Initializing Oap Env")

  var initialized: Boolean = false
  var sparkSession: SparkSession = _

  // This is to enable certain OAP features, like UI, even
  // in non-Spark SQL CLI/ThriftServer conditions
  def initWithoutCreatingSparkSession(): Unit = synchronized {
    if (!initialized && !Utils.isTesting) {
      val sc = SparkContext.getOrCreate()
      sc.addSparkListener(new OapListener)
      this.sparkSession = SparkSession.getActiveSession.get
      sc.ui.foreach(new OapTab(_))
      initialized = true
    }
  }
}
