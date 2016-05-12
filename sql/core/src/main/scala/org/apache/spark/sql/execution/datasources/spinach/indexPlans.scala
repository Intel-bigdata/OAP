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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.{IndexColumn, TableIdentifier}
import org.apache.spark.sql.execution.RunnableCommand

/**
 * Creates an index for table on indexColumns
 */
case class CreateIndex(
    indexName: String,
    tableName: TableIdentifier,
    indexColumns: Array[IndexColumn],
    ifNotExists: Boolean) extends RunnableCommand with Logging {
  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.catalog
    assert(catalog.tableExists(tableName), s"$tableName not exists")
    val indexIdent = TableIdentifier(indexName)
    if (catalog.tableExists(indexIdent)) {
      val msg = s"already exists a index named $indexName"
      if (ifNotExists) {
        // do nothing
        logWarning(msg)
      } else {
        sys.error(msg)
      }
    } else {
      catalog.lookupRelation(tableName) match {
        case Subquery(_, LogicalRelation(r: SpinachRelation, _)) =>
          // TODO See [[[IndexMarker]]]
          catalog.registerTable(indexIdent, IndexMarker(r))
          r.createIndex(indexName, indexColumns)
        case _ => sys.error("Only support CreateIndex for SpinachRelation")
      }
    }
    Seq.empty
  }
}

/**
 * Drops an index
 */
case class DropIndex(
    indexIdent: String,
    ifExists: Boolean) extends RunnableCommand {

  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.catalog
    if (!catalog.tableExists(TableIdentifier(indexIdent))) {
      if (ifExists) {
        // do nothing
      } else {
        sys.error(s"$indexIdent not found")
      }
    } else {
      catalog.lookupRelation(TableIdentifier(indexIdent)) match {
        case Subquery(_, IndexMarker(r: SpinachRelation)) =>
          r.dropIndex(indexIdent)
        case _ => sys.error("Only support DropIndex for SpinachRelation")
      }
      sqlContext.catalog.unregisterTable(TableIdentifier(indexIdent))
    }
    // TODO delete index file and delete index meta from spinach meta
    Seq.empty
  }
}

/**
 * use as the marker to distinguish between table and index in catalog.
 * TODO should use a separate place and APIs
 */
case class IndexMarker(spinachRelation: SpinachRelation) extends LeafNode {
  override val output: Seq[Attribute] = Seq.empty
}
