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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.io.ColumnStatistics
import org.apache.spark.sql.execution.datasources.oap.utils.OapUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

package object oap {
  type Key = InternalRow

  def order(sf: StructField): Ordering[Key] = GenerateOrdering.create(StructType(Array(sf)))

  // Return if the rowGroup or file can be skipped by min max statistics
  def isSkippedByStatistics(
      columnStats: Array[ColumnStatistics],
      filter: Filter,
      schema: StructType): Boolean = filter match {
    case Or(left, right) =>
      isSkippedByStatistics(columnStats, left, schema) &&
          isSkippedByStatistics(columnStats, right, schema)
    case And(left, right) =>
      isSkippedByStatistics(columnStats, left, schema) ||
          isSkippedByStatistics(columnStats, right, schema)
    case IsNotNull(attribute) =>
      val idx = schema.fieldIndex(attribute)
      val stat = columnStats(idx)
      !stat.hasNonNullValue
    case EqualTo(attribute, handle) =>
      val key = OapUtils.keyFromAny(handle)
      val idx = schema.fieldIndex(attribute)
      val stat = columnStats(idx)
      val comp = order(schema(idx))
      (OapUtils.keyFromBytes(stat.min, schema(idx).dataType), OapUtils.keyFromBytes(
        stat.max, schema(idx).dataType)) match {
        case (Some(v1), Some(v2)) => comp.gt(v1, key) || comp.lt(v2, key)
        case _ => false
      }
    case LessThan(attribute, handle) =>
      val key = OapUtils.keyFromAny(handle)
      val idx = schema.fieldIndex(attribute)
      val stat = columnStats(idx)
      val comp = order(schema(idx))
      OapUtils.keyFromBytes(stat.min, schema(idx).dataType) match {
        case Some(v) => comp.gteq(v, key)
        case None => false
      }
    case LessThanOrEqual(attribute, handle) =>
      val key = OapUtils.keyFromAny(handle)
      val idx = schema.fieldIndex(attribute)
      val stat = columnStats(idx)
      val comp = order(schema(idx))
      OapUtils.keyFromBytes(stat.min, schema(idx).dataType) match {
        case Some(v) => comp.gt(v, key)
        case None => false
      }
    case GreaterThan(attribute, handle) =>
      val key = OapUtils.keyFromAny(handle)
      val idx = schema.fieldIndex(attribute)
      val stat = columnStats(idx)
      val comp = order(schema(idx))
      OapUtils.keyFromBytes(stat.max, schema(idx).dataType) match {
        case Some(v) => comp.lteq(v, key)
        case None => false
      }
    case GreaterThanOrEqual(attribute, handle) =>
      val key = OapUtils.keyFromAny(handle)
      val idx = schema.fieldIndex(attribute)
      val stat = columnStats(idx)
      val comp = order(schema(idx))
      OapUtils.keyFromBytes(stat.max, schema(idx).dataType) match {
        case Some(v) => comp.lt(v, key)
        case None => false
      }
    case _ => false
  }
}

/**
 * To express OAP specific exceptions, including but not limited to indicate unsupported operations,
 * for example: BitMapIndexType only supports one single column
 */
class OapException(message: String, cause: Throwable) extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
