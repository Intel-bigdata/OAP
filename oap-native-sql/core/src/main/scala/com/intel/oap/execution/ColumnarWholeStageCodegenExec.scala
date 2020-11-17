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

import java.io.{ByteArrayInputStream, ObjectInputStream}
import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.execution._
import com.intel.oap.expression._
import com.intel.oap.vectorized._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.{UserAddedJarUtils, Utils}
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import com.intel.oap.vectorized.ExpressionEvaluator
import com.intel.oap.vectorized.BatchIterator
import com.google.common.collect.Lists
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

case class ColumnarCodegenContext(inputSchema: Schema, outputSchema: Schema, root: TreeNode) {}

trait ColumnarCodegenSupport extends SparkPlan {

  /**
   * Whether this SparkPlan supports whole stage codegen or not.
   */
  def supportColumnarCodegen: Boolean = true

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  def inputRDDs: Seq[RDD[ColumnarBatch]]

  def getHashBuildPlans: Seq[SparkPlan]

  def doCodeGen: ColumnarCodegenContext

  def dependentPlanCtx: ColumnarCodegenContext = null

}

case class ColumnarWholeStageCodegenExec(child: SparkPlan)(val codegenStageId: Int)
    extends UnaryExecNode
    with ColumnarCodegenSupport {
  val sparkConf = sparkContext.getConf

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_wholestagecodegen"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build dependencies"),
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // This is not strictly needed because the codegen transformation happens after the columnar
  // transformation but just for consistency
  override def supportColumnarCodegen: Boolean = true

  override def supportsColumnar: Boolean = true

  override def otherCopyArgs: Seq[AnyRef] = Seq(codegenStageId.asInstanceOf[Integer])
  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean): Unit = {
    val res = child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($codegenStageId) ",
      false,
      maxFields,
      printNodeId)
    res
  }

  override def nodeName: String = s"WholeStageCodegenColumnar (${codegenStageId})"
  def uploadAndListJars(signature: String): Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = ColumnarPluginConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
      }
      sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
    } else {
      Seq()
    }

  override def doCodeGen: ColumnarCodegenContext = {
    val childCtx = child.asInstanceOf[ColumnarCodegenSupport].doCodeGen
    val wholeStageCodeGenNode = TreeBuilder.makeFunction(
      s"wholestagecodegen",
      Lists.newArrayList(childCtx.root),
      new ArrowType.Int(32, true))
    ColumnarCodegenContext(childCtx.inputSchema, childCtx.outputSchema, wholeStageCodeGenNode)
  }

  override def getHashBuildPlans: Seq[SparkPlan] = {
    child.asInstanceOf[ColumnarCodegenSupport].getHashBuildPlans
  }

  /**
   * Return built cpp library's signature
   */
  def doBuild: String = {
    var resCtx: ColumnarCodegenContext = null
    try {
      // call native wholestagecodegen build
      resCtx = doCodeGen
    } catch {
      case e: UnsupportedOperationException
          if e.getMessage == "Unsupport to generate native expression from replaceable expression." =>
        logWarning(e.getMessage())
        ""
      case e: Throwable =>
        throw e
    }
    //resCtx = doCodeGen
    if (resCtx != null) {
      val expression =
        TreeBuilder.makeExpression(
          resCtx.root,
          Field.nullable("result", new ArrowType.Int(32, true)))
      val nativeKernel = new ExpressionEvaluator()
      nativeKernel.build(
        resCtx.inputSchema,
        Lists.newArrayList(expression),
        resCtx.outputSchema,
        true)
    } else {
      ""
    }
  }

  override def inputRDDs(): Seq[RDD[ColumnarBatch]] = child match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.inputRDDs
    case _ =>
      throw new UnsupportedOperationException
  }
  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val signature = doBuild
    val listJars = uploadAndListJars(signature)

    val numOutputRows = child.longMetric("numOutputRows")
    val numOutputBatches = child.longMetric("numOutputBatches")
    val totalTime = child.longMetric("processTime")
    val pipelineTime = longMetric("pipelineTime")
    val timeout = ColumnarPluginConfig.getConf(sparkConf).broadcastCacheTimeout

    var build_elapse: Long = 0
    var eval_elapse: Long = 0
    // we should zip all dependent RDDs to current main RDD
    val hashBuildPlans = getHashBuildPlans
    val dependentKernels: ListBuffer[ExpressionEvaluator] = ListBuffer()
    val dependentKernelIterators: ListBuffer[BatchIterator] = ListBuffer()
    val hashRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()
    val serializableObjectHolder: ListBuffer[SerializableObject] = ListBuffer()
    val relationHolder: ListBuffer[ColumnarHashedRelation] = ListBuffer()
    var idx = 0
    var curRDD = inputRDDs()(0)
    while (idx < hashBuildPlans.length) {
      val curHashPlan = hashBuildPlans(idx).asInstanceOf[ColumnarCodegenSupport]

      curRDD = curHashPlan match {
        case p: ColumnarBroadcastHashJoinExec =>
          val fetchTime = p.longMetric("fetchTime")
          val buildTime = p.longMetric("buildTime")
          val buildPlan = p.getBuildPlan
          val buildInputByteBuf = buildPlan.executeBroadcast[ColumnarHashedRelation]()
          curRDD.mapPartitions { iter =>
            // received broadcast value contain a hashmap and raw recordBatch
            val beforeFetch = System.nanoTime()
            val relation = buildInputByteBuf.value.asReadOnlyCopy
            relationHolder += relation
            fetchTime += ((System.nanoTime() - beforeFetch) / 1000000)
            val beforeEval = System.nanoTime()
            val hashRelationObject = relation.hashRelationObj
            serializableObjectHolder += hashRelationObject
            val depIter =
              new CloseableColumnBatchIterator(relation.getColumnarBatchAsIter)
            val ctx = curHashPlan.dependentPlanCtx
            val expression =
              TreeBuilder.makeExpression(
                ctx.root,
                Field.nullable("result", new ArrowType.Int(32, true)))
            val hashRelationKernel = new ExpressionEvaluator()
            hashRelationKernel
              .build(ctx.inputSchema, Lists.newArrayList(expression), true)
            val hashRelationResultIterator = hashRelationKernel.finishByIterator()
            dependentKernelIterators += hashRelationResultIterator
            // we need to set original recordBatch to hashRelationKernel
            while (depIter.hasNext) {
              val dep_cb = depIter.next()
              if (dep_cb.numRows > 0) {
                (0 until dep_cb.numCols).toList.foreach(i =>
                  dep_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
                hashRelationBatchHolder += dep_cb
                val dep_rb = ConverterUtils.createArrowRecordBatch(dep_cb)
                hashRelationResultIterator.processAndCacheOne(ctx.inputSchema, dep_rb)
                ConverterUtils.releaseArrowRecordBatch(dep_rb)
              }
            }
            // we need to set hashRelationObject to hashRelationResultIterator
            hashRelationResultIterator.setHashRelationObject(hashRelationObject)
            build_elapse += (System.nanoTime() - beforeEval)
            buildTime += ((System.nanoTime() - beforeEval) / 1000000)
            dependentKernels += hashRelationKernel
            iter
          }
        case p: ColumnarShuffledHashJoinExec =>
          val buildTime = p.longMetric("buildTime")
          val buildPlan = p.getBuildPlan
          curRDD.zipPartitions(buildPlan.executeColumnar()) { (iter, depIter) =>
            val ctx = curHashPlan.dependentPlanCtx
            val expression =
              TreeBuilder.makeExpression(
                ctx.root,
                Field.nullable("result", new ArrowType.Int(32, true)))
            val hashRelationKernel = new ExpressionEvaluator()
            hashRelationKernel
              .build(ctx.inputSchema, Lists.newArrayList(expression), true)
            var build_elapse_internal: Long = 0
            while (depIter.hasNext) {
              val dep_cb = depIter.next()
              (0 until dep_cb.numCols).toList.foreach(i =>
                dep_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
              hashRelationBatchHolder += dep_cb
              val beforeEval = System.nanoTime()
              val dep_rb = ConverterUtils.createArrowRecordBatch(dep_cb)
              hashRelationKernel.evaluate(dep_rb)
              ConverterUtils.releaseArrowRecordBatch(dep_rb)
              build_elapse += System.nanoTime() - beforeEval
              build_elapse_internal += System.nanoTime() - beforeEval
            }
            buildTime += (build_elapse_internal / 1000000)
            dependentKernels += hashRelationKernel
            dependentKernelIterators += hashRelationKernel.finishByIterator()
            iter
          }
        case _ =>
          throw new UnsupportedOperationException
      }

      idx += 1
    }
    curRDD.mapPartitions { iter =>
      ColumnarPluginConfig.getConf(sparkConf)
      val execTempDir = ColumnarPluginConfig.getTempFile
      val jarList = listJars
        .map(jarUrl => {
          logWarning(s"Get Codegened library Jar ${jarUrl}")
          UserAddedJarUtils.fetchJarFromSpark(
            jarUrl,
            execTempDir,
            s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
            sparkConf)
          s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        })

      val resCtx = doCodeGen
      val expression =
        TreeBuilder.makeExpression(
          resCtx.root,
          Field.nullable("result", new ArrowType.Int(32, true)))
      val nativeKernel = new ExpressionEvaluator(jarList.toList.asJava)
      nativeKernel
        .build(resCtx.inputSchema, Lists.newArrayList(expression), resCtx.outputSchema, true)
      val nativeIterator = nativeKernel.finishByIterator()
      // we need to complete dependency RDD's firstly
      nativeIterator.setDependencies(dependentKernelIterators.toArray)

      var closed = false
      def close = {
        closed = true
        totalTime += (eval_elapse / 1000000)
        pipelineTime += (eval_elapse + build_elapse) / 1000000
        hashRelationBatchHolder.foreach(_.close)
        dependentKernels.foreach(_.close)
        dependentKernelIterators.foreach(_.close)
        nativeKernel.close
        nativeIterator.close
        relationHolder.foreach(r => r.countDownClose(timeout))
      }

      // now we can return this wholestagecodegen iter
      val resultStructType = ArrowUtils.fromArrowSchema(resCtx.outputSchema)
      val res = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          iter.hasNext
        }

        override def next(): ColumnarBatch = {
          val cb = iter.next()
          if (cb.numRows == 0) {
            val resultColumnVectors = ArrowWritableColumnVector
              .allocateColumns(0, resultStructType)
              .toArray
            return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
          val beforeEval = System.nanoTime()
          val input_rb =
            ConverterUtils.createArrowRecordBatch(cb)

          val output_rb = nativeIterator.process(resCtx.inputSchema, input_rb)
          if (output_rb == null) {
            ConverterUtils.releaseArrowRecordBatch(input_rb)
            eval_elapse += System.nanoTime() - beforeEval
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
            return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
          val outputNumRows = output_rb.getLength
          ConverterUtils.releaseArrowRecordBatch(input_rb)
          val output = ConverterUtils.fromArrowRecordBatch(resCtx.outputSchema, output_rb)
          ConverterUtils.releaseArrowRecordBatch(output_rb)
          eval_elapse += System.nanoTime() - beforeEval
          numOutputRows += outputNumRows
          new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
        }
      }
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        close
      })
      new CloseableColumnBatchIterator(res)
    }
  }

}
