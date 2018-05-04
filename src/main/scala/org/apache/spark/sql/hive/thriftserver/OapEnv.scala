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

import java.io.PrintStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.{HiveUtils, OapSessionState}
import org.apache.spark.sql.oap.OapSession
import org.apache.spark.sql.oap.listener.OapListener
import org.apache.spark.sql.oap.ui.OapTab
import org.apache.spark.util.Utils

private[hive] object OapEnv extends Logging {
  logDebug("Initializing Oap Env")

  var sqlContext: SQLContext = _
  var sparkContext: SparkContext = _

  def init() {
    if (sqlContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      val maybeSerializer = sparkConf.getOption("spark.serializer")
      val maybeKryoReferenceTracking = sparkConf.getOption("spark.kryo.referenceTracking")
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)

      sparkConf
        .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))
        .set(
          "spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set(
          "spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      val sparkSession = OapSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
      sparkContext = sparkSession.sparkContext
      sqlContext = sparkSession.sqlContext

      val sessionState = sparkSession.sessionState.asInstanceOf[OapSessionState]
      sessionState.metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
      sessionState.metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
      sessionState.metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))
      sparkSession.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)
    }

    sparkContext.addSparkListener(new OapListener)

    SparkSQLEnv.sparkContext = sparkContext
    SparkSQLEnv.sqlContext = sqlContext

    sparkContext.ui.foreach(new OapTab(_))
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    SparkSQLEnv.stop()
  }
}
