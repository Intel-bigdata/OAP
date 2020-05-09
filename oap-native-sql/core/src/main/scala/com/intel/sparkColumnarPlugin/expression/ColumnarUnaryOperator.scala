package com.intel.sparkColumnarPlugin.expression

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

object ColumnarUnaryOperator {

  def create(child: Expression, original: Expression): Expression = original match {
    case i: IsNotNull =>
      new ColumnarIsNotNull(child, i)
    case y: Year =>
      new ColumnarYear(child, y)
    case n: Not =>
      new ColumnarNot(child, n)
    case a: Abs =>
      new ColumnarAbs(child, a)
    case c: Cast =>
      child
    case a: KnownFloatingPointNormalized =>
      child
    case a: NormalizeNaNAndZero =>
      child
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
