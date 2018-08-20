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

package org.apache.spark.sql.oap.adapter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation}
import org.apache.spark.sql.types.StructType

object FileSourceScanExecAdapter extends Logging{
  def createFileSourceScanExec(
      relation: HadoopFsRelation,
      output: Seq[Attribute],
      requiredSchema: StructType,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      metastoreTableIdentifier: Option[TableIdentifier]): FileSourceScanExec = {
    val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
    logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")
    FileSourceScanExec(
      relation,
      output,
      requiredSchema,
      partitionFilters,
      pushedDownFilters,
      metastoreTableIdentifier)
  }
}
