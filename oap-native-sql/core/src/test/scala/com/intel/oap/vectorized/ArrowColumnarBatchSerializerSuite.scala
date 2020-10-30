package com.intel.oap.vectorized

import java.io.FileInputStream

import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.SparkFunSuite

class ArrowColumnarBatchSerializerSuite extends SparkFunSuite with SharedSparkSession {

  protected var avgBatchNumRows: SQLMetric = _
  protected var outputNumRows: SQLMetric = _

  override def beforeEach() = {
    avgBatchNumRows = SQLMetrics.createAverageMetric(
      spark.sparkContext,
      "test serializer avg read batch num rows")
    outputNumRows =
      SQLMetrics.createAverageMetric(spark.sparkContext, "test serializer number of output rows")
  }

  test("deserialize all null") {
    val input = getTestResourcePath("test-data/native-splitter-output-all-null")
    val serializer =
      new ArrowColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream =
      serializer.deserializeStream(new FileInputStream(input))

    val kv = deserializedStream.asKeyValueIterator
    var length = 0
    kv.foreach {
      case (_, batch: ColumnarBatch) =>
        length += 1
        assert(batch.numRows == 4)
        assert(batch.numCols == 3)
        (0 until batch.numCols).foreach { i =>
          val valueVector =
            batch
              .column(i)
              .asInstanceOf[ArrowWritableColumnVector]
              .getValueVector
          assert(valueVector.getValueCount == batch.numRows)
          assert(valueVector.getNullCount === batch.numRows)
        }
    }
    assert(length == 2)
    deserializedStream.close()
  }

  test("deserialize nullable string") {
    val input = getTestResourcePath("test-data/native-splitter-output-nullable-string")
    val serializer =
      new ArrowColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream =
      serializer.deserializeStream(new FileInputStream(input))

    val kv = deserializedStream.asKeyValueIterator
    var length = 0
    kv.foreach {
      case (_, batch: ColumnarBatch) =>
        length += 1
        assert(batch.numRows == 8)
        assert(batch.numCols == 3)
        (0 until batch.numCols).foreach { i =>
          val valueVector =
            batch
              .column(i)
              .asInstanceOf[ArrowWritableColumnVector]
              .getValueVector
          assert(valueVector.getValueCount == batch.numRows)
        }
    }
    assert(length == 2)
    deserializedStream.close()
  }
}
