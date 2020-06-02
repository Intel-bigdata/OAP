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

package org.apache.spark.sql.execution

import java.util.Random

import com.intel.sparkColumnarPlugin.vectorized.ArrowColumnarBatchSerializer
import com.intel.sparkColumnarPlugin.vectorized.ArrowWritableColumnVector
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.{
  SQLMetric,
  SQLMetrics,
  SQLShuffleReadMetricsReporter,
  SQLShuffleWriteMetricsReporter
}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.{HashPartitioner, Partitioner, ShuffleDependency, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class ColumnarShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    canChangeNumPartitions: Boolean = true)
    extends Exchange {

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics
      .createSizeMetric(sparkContext, "data size")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "ColumnarExchange"

  override def supportsColumnar: Boolean = true

  // Disable code generation
  @transient lazy val inputRDD: RDD[ColumnarBatch] = child.executeColumnar()

  private val serializer: Serializer = new ArrowColumnarBatchSerializer

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    ColumnarShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics)
  }

  def createShuffledRDD(partitionStartIndices: Option[Array[Int]]): ShuffledColumnarBatchRDD = {
    new ShuffledColumnarBatchRDD(shuffleDependency, readMetrics, partitionStartIndices)
  }

  private var cachedShuffleRDD: ShuffledColumnarBatchRDD = null

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = createShuffledRDD(None)
    }
    cachedShuffleRDD
  }
}

object ColumnarShuffleExchangeExec {

  def prepareShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      enableArrowColumnVector: Boolean = true)
      : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {

    assert(enableArrowColumnVector, "only support arrow column vector")

    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        new Partitioner {
          override def numPartitions: Int = n
          // For HashPartitioning, the partitioning key is already a valid partition ID, as we use
          // `HashPartitioning.partitionIdExpression` to produce partitioning key.
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle RangePartitioning.
      // TODO: Handle BroadcastPartitioning.
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    val schema = StructType.fromAttributes(outputAttributes)

    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
        row => projection(row).getInt(0)
      case RangePartitioning(sortingExpressions, _) =>
        val projection =
          UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
        row => projection(row)
      case SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }

    def getPartitionIdMask(cb: ColumnarBatch): Array[Int] = {
      val getPartitionKey = getPartitionKeyExtractor()
      val rowIterator = cb.rowIterator().asScala
      val partitionIdMask = new Array[Int](cb.numRows())

      rowIterator.zipWithIndex.foreach {
        case (row, idx) =>
          partitionIdMask(idx) = part.getPartition(getPartitionKey(row))
      }
      partitionIdMask
    }

    def createColumnVectors(numRows: Int): Array[WritableColumnVector] = {
      val vectors: Seq[WritableColumnVector] =
        ArrowWritableColumnVector.allocateColumns(numRows, schema)
      vectors.toArray
    }

    def partitionIdToColumnVectors(
        partitionCounter: PartitionCounter): Map[Int, Array[WritableColumnVector]] = {
      val columnVectorsWithPartitionId = mutable.Map[Int, Array[WritableColumnVector]]()
      for (pid <- partitionCounter.keys) {
        val numRows = partitionCounter(pid)
        if (numRows > 0) {
          columnVectorsWithPartitionId.update(pid, createColumnVectors(numRows))
        }
      }
      columnVectorsWithPartitionId.toMap[Int, Array[WritableColumnVector]]
    }

    val rddWithPartitionIds: RDD[Product2[Int, ColumnarBatch]] = {
      rdd.mapPartitionsWithIndexInternal(
        (_, cbIterator) => {
          val converters = new RowToColumnConverter(schema)
          cbIterator.flatMap {
            cb =>
              val partitionCounter = new PartitionCounter
              val partitionIdMask = getPartitionIdMask(cb)
              partitionCounter.update(partitionIdMask)

              val toNewVectors = partitionIdToColumnVectors(partitionCounter)
              val rowIterator = cb.rowIterator().asScala
              rowIterator.zipWithIndex.foreach { rowWithIdx =>
                val idx = rowWithIdx._2
                val pid = partitionIdMask(idx)
                converters.convert(rowWithIdx._1, toNewVectors(pid))
              }
              toNewVectors.toSeq.map {
                case (pid, vectors) =>
                  (pid, new ColumnarBatch(vectors.toArray, partitionCounter(pid)))
              }
          }
        },
        isOrderSensitive = isOrderSensitive)
    }

    val dependency =
      new ShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))

    dependency
  }
}

private class PartitionCounter {
  private val pidCounter = mutable.Map[Int, Int]()

  def size: Int = pidCounter.size

  def keys: Iterable[Int] = pidCounter.keys

  def update(partitionIdMask: Array[Int]): Unit = {
    partitionIdMask.foreach { partitionId =>
      pidCounter.update(partitionId, pidCounter.get(partitionId) match {
        case Some(cnt) => cnt + 1
        case None => 1
      })
    }
  }

  def apply(partitionId: Int): Int = pidCounter.getOrElse(partitionId, 0)
}
