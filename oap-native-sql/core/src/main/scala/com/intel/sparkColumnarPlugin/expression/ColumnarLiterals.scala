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

package com.intel.sparkColumnarPlugin.expression

import com.google.common.collect.Lists
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.DateUnit

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarLiteral(lit: Literal)
    extends Literal(lit.value, lit.dataType)
    with ColumnarExpression {

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val resultType = CodeGeneration.getResultType(dataType)
    dataType match {
      case t: StringType =>
        (TreeBuilder.makeStringLiteral(value.toString().asInstanceOf[String]), resultType)
      case t: IntegerType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[Integer]), resultType)
      case t: LongType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Long]), resultType)
      case t: DoubleType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Double]), resultType)
      case d: DecimalType =>
        val v = value.asInstanceOf[Decimal]
        (TreeBuilder.makeDecimalLiteral(v.toString, v.precision, v.scale), resultType)
      case d: DateType =>
        val origIntNode = TreeBuilder.makeLiteral(value.asInstanceOf[Integer])
        val dateNode = TreeBuilder.makeFunction("castDATE", Lists.newArrayList(origIntNode), new ArrowType.Date(DateUnit.DAY))
        (dateNode, new ArrowType.Date(DateUnit.DAY))
      case b: BooleanType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Boolean]), resultType)
    }
  }
}
