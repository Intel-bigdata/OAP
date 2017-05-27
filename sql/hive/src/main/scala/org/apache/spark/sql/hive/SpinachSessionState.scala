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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.parser.{ParserInterface, SqlBaseParser}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}

class SpinachSessionState(sparkSession: SpinachSession) extends HiveSessionState(sparkSession) {
  self =>
  // TODO extends `experimentalMethods.extraStrategies`
  private lazy val sharedState: HiveSharedState = {
    sparkSession.sharedState.asInstanceOf[HiveSharedState]
  }
  override lazy val metadataHive: HiveClient = sharedState.metadataHive.newSession()

  override lazy val sqlParser: ParserInterface = new SpinachSqlParser(conf)
}

/**
 * Concrete parser for Spark SQL statements.
 */
class SpinachSqlParser(conf: SQLConf) extends SparkSqlParser(conf) {
  private val substitutor = new VariableSubstitution(conf)

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}
