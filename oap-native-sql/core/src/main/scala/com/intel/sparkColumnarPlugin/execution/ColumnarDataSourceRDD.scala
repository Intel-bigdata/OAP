package com.intel.sparkColumnarPlugin.execution

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, PartitionReader}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class DataSourceRDDPartition(val index: Int, val inputPartition: InputPartition)
  extends Partition with Serializable

// TODO: we should have 2 RDDs: an RDD[InternalRow] for row-based scan, an `RDD[ColumnarBatch]` for
// columnar scan.
class ColumnarDataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    partitionReaderFactory: PartitionReaderFactory,
    columnarReads: Boolean,
    scanTime: SQLMetric)
  extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new DataSourceRDDPartition(index, inputPartition)
    }.toArray
  }

  private def castPartition(split: Partition): DataSourceRDDPartition = split match {
    case p: DataSourceRDDPartition => p
    case _ => throw new SparkException(s"[BUG] Not a DataSourceRDDPartition: $split")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val inputPartition = castPartition(split).inputPartition
    val reader: PartitionReader[_] = if (columnarReads) {
      partitionReaderFactory.createColumnarReader(inputPartition)
    } else {
      partitionReaderFactory.createReader(inputPartition)
    }

    val rddId = this
    context.addTaskCompletionListener[Unit](_ => reader.close())
    val iter = new Iterator[Any] {
      private[this] var valuePrepared = false

      override def hasNext: Boolean = {
        if (!valuePrepared) {
          try {
            val beforeScan = System.nanoTime()
            valuePrepared = reader.next()
            scanTime += (System.nanoTime() - beforeScan) / (1000 * 1000)
          } catch {
            case e =>
              val errmsg = e.getStackTrace.mkString("\n")
              logError(s"hasNext got exception: $errmsg")
              valuePrepared = false
          }
        }
        valuePrepared
      }

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        valuePrepared = false
        reader.get()
      }
    }
    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, iter.asInstanceOf[Iterator[ColumnarBatch]])
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }
}
