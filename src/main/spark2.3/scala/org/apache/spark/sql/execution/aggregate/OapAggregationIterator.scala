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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType

/**
 * An iterator used to evaluate aggregate functions for OAP index Data to
 * accelerate the GROUP BY query. It operates on [[UnsafeRow]]s.
 * Basically it is much like TungstenAggregationIterator, with only few
 * changes:
 * 1. Remove hashmap related operations, include hashmap itself and code to
 *    do potential sort based aggregation.
 * 2. Add code to process a group of [[UnsafeRow]]s which belongs to same
 *    index category.
 *
 * NOTE: the reason why it can NOT inherit from TungstenAggregationIterator
 * is lot of functions are private to TungstenAggregationIterator, even worse
 * the Iterator.hasNext() is final at there. Also it seems not a good idea to
 * separate TungstenAggregationIterator out of Spark.
 */
class OapAggregationIterator(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    testFallbackStartsAt: Option[(Int, Int)],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric)
  extends AggregationIterator(
    partIndex: Int,
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most two times (when we create the hash map,
  // and when we create the re-used buffer for sort-based aggregation).
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericInternalRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // Creates a function used to generate output rows.
  override protected def generateResultProjection(): (UnsafeRow, InternalRow) => UnsafeRow = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.nonEmpty && !modes.contains(Final) && !modes.contains(Complete)) {
      // Fast path for partial aggregation, UnsafeRowJoiner is usually faster than projection
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
      val bufferSchema = StructType.fromAttributes(bufferAttributes)
      val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        unsafeRowJoiner.join(currentGroupingKey, currentBuffer.asInstanceOf[UnsafeRow])
      }
    } else {
      super.generateResultProjection()
    }
  }

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  private[this] val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()
  private[this] val groupAggregationBuffer: UnsafeRow = initialAggregationBuffer.copy()

  var currGroupHead : InternalRow = _

  // As rows are returned by index, so they are group-ed.
  // We call this for one group. Processing stops after one
  // group scan is done, and next group process starts when
  // iterator next get called.
  private def processOneGroupInputs(): (UnsafeRow, UnsafeRow) = {
    // Init agg buffer for a new group.
    groupAggregationBuffer.copyFrom(initialAggregationBuffer)

    // Set the group key and process the first row in current group.
    val currGroupingKey = groupingProjection.apply(currGroupHead).copy()
    processRow(groupAggregationBuffer, currGroupHead)

    var isSameGroup = true
    while(isSameGroup && inputIter.hasNext) {
      val nextInput = inputIter.next()
      val nextGroupingKey = groupingProjection.apply(nextInput)
      if (currGroupingKey.equals(nextGroupingKey)) {
        // process next row which belongs to same group
        processRow(groupAggregationBuffer, nextInput)
      } else {
        isSameGroup = false
        currGroupHead = nextInput
      }
    }

    // No more group, set currGroupHead to null to finish process
    if (isSameGroup) {
      currGroupHead = null
    }

    (currGroupingKey, groupAggregationBuffer)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////
  if (currGroupHead == null && inputIter.hasNext) {
    currGroupHead = inputIter.next()
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    currGroupHead != null
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      val res = {
        val aggBuffer : (UnsafeRow, UnsafeRow) = processOneGroupInputs()
        // We did not fall back to sort-based aggregation.
        val result = generateOutput(aggBuffer._1, aggBuffer._2)
        // TODO: reconsider if we can save this copy.
        result.copy()
      }

      numOutputRows += 1
      // If this is the last record, update the task's peak memory usage.
      // So far it has no map/sorter/spill memory.
      if (!hasNext) {
        val mapMemory = 0L
        val sorterMemory = 0L
        val maxMemory = Math.max(mapMemory, sorterMemory)
        val metrics = TaskContext.get().taskMetrics()
        peakMemory += maxMemory
        spillSize += 0L
        metrics.incPeakExecutionMemory(maxMemory)
      }
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    throw new IllegalStateException(
      "This method should not be called when groupingExpressions is not empty.")
  }
}
