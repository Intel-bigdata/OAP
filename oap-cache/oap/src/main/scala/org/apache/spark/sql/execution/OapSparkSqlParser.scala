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

import scala.collection.JavaConverters._

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree._

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{OapSqlBaseBaseListener, OapSqlBaseBaseVisitor, OapSqlBaseLexer, OapSqlBaseParser, ParseErrorListener, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.OapSqlBaseParser._
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.ParserUtils.{string, withOrigin}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A SQL parser that tries to parse OAP commands. If failing to parse the SQL text, it will
 * forward the call to `delegate`.
 */
class OapSparkSqlParser(session: SparkSession, delegate: ParserInterface) extends ParserInterface {

  lazy val conf: SQLConf = session.sessionState.conf
  lazy val builder = new OapSqlBaseAstBuilder(conf)

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    builder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ => delegate.parsePlan(sqlText)
    }
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  // scalastyle:off line.size.limit
  /**
   * Fork from `org.apache.spark.sql.catalyst.parser.AbstractSqlParser#parse(java.lang.String, scala.Function1)`.
   *
   * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L81
   */
  // scalastyle:on
  protected def parse[T](command: String)(toResult: OapSqlBaseParser => T): T = {
    val lexer = new OapSqlBaseLexer(
      new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new OapSqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  /** Similar to `parseDataType`, but without CHAR/VARCHAR replacement. */
  override def parseRawDataType(sqlText: String): DataType = delegate.parseRawDataType(sqlText)

}

/**
 * Define how to convert an AST generated from `OapSqlBase.g4` to a `LogicalPlan`. The parent
 * class `OapSqlBaseBaseVisitor` defines all visitXXX methods generated from `#` instructions in
 * `OapSqlBase.g4` (such as `#oapCreateIndex`).
 */
class OapSqlBaseAstBuilder(conf: SQLConf) extends OapSqlBaseBaseVisitor[AnyRef] {

  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null

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

  override def visitTableIdentifier(ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  protected def visitNonOptionalPartitionSpec(
      ctx: PartitionSpecContext): Map[String, String] = withOrigin(ctx) {
    visitPartitionSpec(ctx).map {
      case (key, None) => throw new ParseException(s"Found an empty partition key '$key'.", ctx)
      case (key, Some(value)) => key -> value
    }
  }

  override def visitPartitionSpec(
                                   ctx: PartitionSpecContext): Map[String, Option[String]] = withOrigin(ctx) {
    val parts = ctx.partitionVal.asScala.map { pVal =>
      val name = pVal.identifier.getText
      val value = Option(pVal.constant).map(visitStringConstant)
      name -> value
    }
    // Before calling `toMap`, we check duplicated keys to avoid silently ignore partition values
    // in partition spec like PARTITION(a='1', b='2', a='3'). The real semantical check for
    // partition columns will be done in analyzer.
    checkDuplicateKeys(parts, ctx)
    parts.toMap
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  protected def visitStringConstant(ctx: ConstantContext): String = withOrigin(ctx) {
    ctx match {
      case s: StringLiteralContext => createString(s)
      case o => o.getText
    }
  }

  private def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.STRING().asScala.map(stringWithoutUnescape).mkString
    } else {
      ctx.STRING().asScala.map(string).mkString
    }
  }

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }
}

// scalastyle:off line.size.limit
/**
 * Fork from `org.apache.spark.sql.catalyst.parser.UpperCaseCharStream`.
 *
 * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L157
 */
// scalastyle:on
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

// scalastyle:off line.size.limit
/**
 * Fork from `org.apache.spark.sql.catalyst.parser.PostProcessor`.
 *
 * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L248
 */
// scalastyle:on
case object PostProcessor extends OapSqlBaseBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }
  private def replaceTokenByIdentifier(
      ctx: ParserRuleContext,
      stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      OapSqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))

  }
}
