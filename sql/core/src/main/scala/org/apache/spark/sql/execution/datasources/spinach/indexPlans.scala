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

package org.apache.spark.sql.execution.datasources.spinach

import org.apache.spark.Logging
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.execution.RunnableCommand

/**
 * Creates an index for table on indexColumns
 */
case class CreateIndex(
    indexName: String,
    tableName: TableIdentifier,
    indexColumns: Array[IndexColumn],
    allowExists: Boolean) extends RunnableCommand with Logging {
  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.catalog
    assert(catalog.tableExists(tableName), s"$tableName not exists")
    catalog.lookupRelation(tableName) match {
      case Subquery(_, LogicalRelation(r: SpinachRelation, _)) =>
        r.createIndex(indexName, indexColumns, allowExists)
      case _ => sys.error("Only support CreateIndex for SpinachRelation")
    }
    Seq.empty
  }
}

/**
 * Drops an index
 */
case class DropIndex(
    indexIdent: String,
    tableIdentifier: TableIdentifier,
    allowNotExists: Boolean) extends RunnableCommand {

  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.catalog
    catalog.lookupRelation(tableIdentifier) match {
      case Subquery(_, LogicalRelation(r: SpinachRelation, _)) =>
        r.dropIndex(indexIdent, allowNotExists)
      case _ => sys.error("Only support DropIndex for SpinachRelation")
    }
    Seq.empty
  }
}
