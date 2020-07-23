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

package com.intel.oap.execution

import com.intel.oap.expression._
import com.intel.oap.vectorized._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.TaskContext

case class ColumnarConditionProjectExec(condition: Expression, projectList: Seq[Expression], child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper with Logging {

  def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  val notNullAttributes = if (condition != null) {
    val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
      case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
      case _ => false
    }
    notNullPreds.flatMap(_.references).distinct.map(_.exprId)
  } else {
    null
  }
  override def output: Seq[Attribute] = if (projectList != null) {
    val res = projectList.map(expr => ConverterUtils.getAttrFromExpr(expr))
    res
  } else if (condition != null){
    val res = child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
    res
  } else {
    val res = child.output.map { a =>
      a
    }
    res
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def canEqual(that: Any): Boolean = false

  protected override def doExecute(): org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of Input batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_condproject"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputBatches = longMetric("numInputBatches")
    val procTime = longMetric("processTime")
    numOutputRows.set(0)
    numOutputBatches.set(0)
    numInputBatches.set(0)

    child.executeColumnar().mapPartitions { iter =>
      val condProj = ColumnarConditionProjector.create(
        condition,
        projectList,
        child.output,
        numInputBatches,
        numOutputBatches,
        numOutputRows,
        procTime)
      TaskContext
        .get()
        .addTaskCompletionListener[Unit]((tc: TaskContext) => {
          condProj.close()
        })
      new CloseableColumnBatchIterator(condProj.createIterator(iter))
    }
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarConditionProjectExec]
  }
}
