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

import java.util.Locale

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.StructType

/**
 * Concrete parser for Spark SQL statements.
 */
 class OapSparkSqlParser extends AbstractSqlParser {
  lazy val conf = SparkSession.getActiveSession.get.sessionState.conf
  lazy val astBuilder = new OapSparkSqlAstBuilder(conf)

  private lazy val substitutor = new VariableSubstitution(conf)

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
 }

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class OapSparkSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  /**
   * Create an index. Create a [[CreateIndexCommand]] command.
   *
   * {{{
   *   CREATE OINDEX [IF NOT EXISTS] indexName ON tableName (col1 [ASC | DESC], col2, ...)
   *   [USING (BTREE | BITMAP)] [PARTITION (partcol1=val1, partcol2=val2 ...)]
   * }}}
   */
  override def visitOapCreateIndex(ctx: OapCreateIndexContext): LogicalPlan =
    withOrigin(ctx) {
      CreateIndexCommand(
        ctx.IDENTIFIER.getText,
        visitTableIdentifier(ctx.tableIdentifier()),
        visitIndexCols(ctx.indexCols),
        ctx.EXISTS != null, visitIndexType(ctx.indexType),
        Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
    }

  /**
   * Drop an index. Create a [[DropIndexCommand]] command.
   *
   * {{{
   *   DROP OINDEX [IF EXISTS] indexName on tableName [PARTITION (partcol1=val1, partcol2=val2 ...)]
   * }}}
   */
  override def visitOapDropIndex(ctx: OapDropIndexContext): LogicalPlan = withOrigin(ctx) {
    DropIndexCommand(
      ctx.IDENTIFIER.getText,
      visitTableIdentifier(ctx.tableIdentifier),
      ctx.EXISTS != null,
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
  }

  override def visitIndexCols(ctx: IndexColsContext): Array[IndexColumn] = withOrigin(ctx) {
    ctx.indexCol.toArray(new Array[IndexColContext](ctx.indexCol.size)).map(visitIndexCol)
  }

  override def visitIndexCol(ctx: IndexColContext): IndexColumn = withOrigin(ctx) {
    IndexColumn(ctx.identifier.getText, ctx.DESC == null)
  }

  override def visitIndexType(ctx: IndexTypeContext): OapIndexType = if (ctx == null) {
    BTreeIndexType
  } else {
    withOrigin(ctx) {
      if (ctx.BTREE != null) {
        BTreeIndexType
      } else {
        BitMapIndexType
      }
    }
  }

  override def visitOapRefreshIndices(ctx: OapRefreshIndicesContext): LogicalPlan =
    withOrigin(ctx) {
      RefreshIndexCommand(
        visitTableIdentifier(ctx.tableIdentifier),
        Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
    }

  override def visitOapShowIndex(ctx: OapShowIndexContext): LogicalPlan = withOrigin(ctx) {
    val tableName = visitTableIdentifier(ctx.tableIdentifier)
    OapShowIndexCommand(tableName, tableName.identifier)
  }

  override def visitOapCheckIndex(ctx: OapCheckIndexContext): LogicalPlan =
    withOrigin(ctx) {
      val tableIdentifier = visitTableIdentifier(ctx.tableIdentifier)
      OapCheckIndexCommand(
        tableIdentifier,
        tableIdentifier.identifier,
        Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
    }

  override def visitOapDisableIndex(ctx: OapDisableIndexContext): LogicalPlan =
    OapDisableIndexCommand(ctx.IDENTIFIER.getText)

  override def visitOapEnableIndex(ctx: OapEnableIndexContext): LogicalPlan =
    OapEnableIndexCommand(ctx.IDENTIFIER.getText)

}
