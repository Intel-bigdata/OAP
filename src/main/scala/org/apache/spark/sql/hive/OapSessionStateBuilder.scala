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
import org.apache.spark.sql.execution.{SparkPlanner, SparkSqlParser}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.execution.datasources.oap.OapStrategies
import org.apache.spark.sql.internal.{SessionState, SQLConf, VariableSubstitution}

class OapSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(session, parentState) {

  override lazy val sqlParser: ParserInterface = new OapSqlParser(conf)

  override protected def planner: SparkPlanner = {
    new SparkPlanner(session.sparkContext, conf, experimentalMethods)
      with HiveStrategies
      with OapStrategies {
      override val sparkSession: SparkSession = session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++
          extraPlanningStrategies ++
          oapStrategies ++ Seq(
          FileSourceStrategy,
          DataSourceStrategy(conf),
          SpecialLimits,
          InMemoryScans,
          HiveTableScans,
          Scripts,
          Aggregation,
          JoinSelection,
          BasicOperators
        )
      }
    }
  }

  override protected def newBuilder: NewBuilder = new OapSessionStateBuilder(_, _)
}


/**
 * Concrete parser for Spark SQL statements.
 */
class OapSqlParser(conf: SQLConf) extends SparkSqlParser(conf) {
  private val substitutor = new VariableSubstitution(conf)

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}
