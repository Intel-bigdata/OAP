package com.intel.oap.expression

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarConcat(exps: Seq[Expression], original: Expression)
    extends Concat(exps: Seq[Expression])
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val iter: Iterator[Expression] = exps.iterator
    val exp = iter.next()
    val iterFaster: Iterator[Expression] = exps.iterator
    iterFaster.next()
    iterFaster.next()

    val (exp_node, expType): (TreeNode, ArrowType) =
      exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Utf8()
    val funcNode = TreeBuilder.makeFunction("concat",
      Lists.newArrayList(exp_node, rightNode(args, exps, iter, iterFaster)), resultType)
    (funcNode, expType)
  }

  def rightNode(args: java.lang.Object, exps: Seq[Expression],
                iter: Iterator[Expression], iterFaster: Iterator[Expression]): TreeNode = {
    if (!iterFaster.hasNext) {
      // When iter reaches the last but one expression
      val (exp_node, expType): (TreeNode, ArrowType) =
        exps.last.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      exp_node
    } else {
      val exp = iter.next()
      iterFaster.next()
      val (exp_node, expType): (TreeNode, ArrowType) =
        exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val resultType = new ArrowType.Utf8()
      val funcNode = TreeBuilder.makeFunction("concat",
        Lists.newArrayList(exp_node, rightNode(args, exps, iter, iterFaster)), resultType)
      funcNode
    }
  }
}

object ColumnarConcatOperator {

  def create(exps: Seq[Expression], original: Expression): Expression = original match {
    case c: Concat =>
      new ColumnarConcat(exps, original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
