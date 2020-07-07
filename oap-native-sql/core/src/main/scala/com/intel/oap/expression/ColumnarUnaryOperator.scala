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

package com.intel.oap.expression

import com.google.common.collect.Lists

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.DateUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * A version of add that supports columnar processing for longs.
 */
class ColumnarIsNotNull(child: Expression, original: Expression)
    extends IsNotNull(child: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction("isnotnull", Lists.newArrayList(child_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarIsNull(child: Expression, original: Expression)
    extends IsNotNull(child: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction("isnull", Lists.newArrayList(child_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarYear(child: Expression, original: Expression)
    extends Year(child: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Int(32, true)
    //FIXME(): requires utf8()/int64() as input
    val cast_func = TreeBuilder.makeFunction("castDATE",
      Lists.newArrayList(child_node), new ArrowType.Date(DateUnit.MILLISECOND))
    val funcNode =
      TreeBuilder.makeFunction("extractYear", Lists.newArrayList(cast_func), new ArrowType.Int(64, true))
    val castNode =
      TreeBuilder.makeFunction("castINT", Lists.newArrayList(funcNode), resultType)
    (castNode, resultType)
  }
}

class ColumnarNot(child: Expression, original: Expression)
    extends Not(child: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction("not", Lists.newArrayList(child_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarAbs(child: Expression, original: Expression)
  extends Abs(child: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    val funcNode =
      TreeBuilder.makeFunction("abs", Lists.newArrayList(child_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarUpper(child: Expression, original: Expression)
  extends Upper(child: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Utf8()
    val funcNode =
      TreeBuilder.makeFunction("upper", Lists.newArrayList(child_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarCast(child: Expression, datatype: DataType, timeZoneId: Option[String], original: Expression)
  extends Cast(child: Expression, datatype: DataType, timeZoneId: Option[String])
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = CodeGeneration.getResultType(dataType)
    if (dataType == StringType) {
      //TODO: fix cast uft8
      (child_node, childType)
    } else if (dataType == IntegerType) {
      val funcNode =
        TreeBuilder.makeFunction("castINT", Lists.newArrayList(child_node), resultType)
      (funcNode, resultType)
    } else if (dataType == LongType) {
      val funcNode =
        TreeBuilder.makeFunction("castBIGINT", Lists.newArrayList(child_node), resultType)
      (funcNode, resultType)
      //(child_node, childType)
    } else if (dataType == FloatType) {
      val funcNode =
        TreeBuilder.makeFunction("castFLOAT4", Lists.newArrayList(child_node), resultType)
      (funcNode, resultType)
    } else if (dataType == DoubleType) {
      val funcNode =
        TreeBuilder.makeFunction("castFLOAT8", Lists.newArrayList(child_node), resultType)
      (funcNode, resultType)
    } else if (dataType == DateType) {
      val funcNode =
        TreeBuilder.makeFunction("castDATE", Lists.newArrayList(child_node), resultType)
      (funcNode, resultType)
    }  else if (dataType == DecimalType) {
      throw new UnsupportedOperationException(s"not currently supported: ${dataType}.")
    } else {
      throw new UnsupportedOperationException(s"not currently supported: ${dataType}.")
    }
  }
}

object ColumnarUnaryOperator {

  def create(child: Expression, original: Expression): Expression = original match {
    case in: IsNull =>
      new ColumnarIsNull(child, in)
    case i: IsNotNull =>
      new ColumnarIsNotNull(child, i)
    case y: Year =>
      new ColumnarYear(child, y)
    case n: Not =>
      new ColumnarNot(child, n)
    case a: Abs =>
      new ColumnarAbs(child, a)
    case u: Upper =>
      new ColumnarUpper(child, u)
    case c: Cast =>
      new ColumnarCast(child, c.dataType, c.timeZoneId, c)
    case a: KnownFloatingPointNormalized =>
      child
    case a: NormalizeNaNAndZero =>
      child
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
