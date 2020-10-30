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

package org.apache.spark.shuffle

import java.io.File
import java.nio.file.Files

import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized.{ArrowWritableColumnVector, NativePartitioning}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.arrow.vector.{FieldVector, IntVector}
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.sort.ColumnarShuffleHandle
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.{Mock, MockitoAnnotations}

import scala.collection.JavaConverters._

class ColumnarShuffleWriterSuite extends SharedSparkSession {
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency
    : ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = _

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test ColumnarShuffleWriter")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")

  private var taskMetrics: TaskMetrics = _
  private var tempDir: File = _
  private var outputFile: File = _

  private var shuffleHandle: ColumnarShuffleHandle[Int, ColumnarBatch] = _
  private val schema = new Schema(
    List(
      Field.nullable("f_1", new ArrowType.Int(32, true)),
      Field.nullable("f_2", new ArrowType.Int(32, true)),
    ).asJava)
  private val allocator = new RootAllocator(Long.MaxValue)
  private val numPartitions = 11

  override def beforeEach() = {
    super.beforeEach()

    tempDir = Utils.createTempDir()
    outputFile = File.createTempFile("shuffle", null, tempDir)
    taskMetrics = new TaskMetrics

    MockitoAnnotations.initMocks(this)

    shuffleHandle =
      new ColumnarShuffleHandle[Int, ColumnarBatch](shuffleId = 0, dependency = dependency)

    when(dependency.partitioner).thenReturn(new HashPartitioner(numPartitions))
    when(dependency.serializer).thenReturn(new JavaSerializer(sparkConf))
    when(dependency.nativePartitioning).thenReturn(
      new NativePartitioning("rr", numPartitions, ConverterUtils.getSchemaBytesBuf(schema)))
    when(dependency.dataSize)
      .thenReturn(SQLMetrics.createSizeMetric(spark.sparkContext, "data size"))
    when(dependency.bytesSpilled)
      .thenReturn(SQLMetrics.createSizeMetric(spark.sparkContext, "shuffle bytes spilled"))
    when(dependency.numInputRows)
      .thenReturn(SQLMetrics.createMetric(spark.sparkContext, "number of input rows"))
    when(dependency.splitTime)
      .thenReturn(SQLMetrics.createNanoTimingMetric(spark.sparkContext, "totaltime_split"))
    when(dependency.spillTime)
      .thenReturn(SQLMetrics.createNanoTimingMetric(spark.sparkContext, "totaltime_spill"))
    when(dependency.computePidTime)
      .thenReturn(SQLMetrics.createNanoTimingMetric(spark.sparkContext, "totaltime_computepid"))
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)

    doAnswer { (invocationOnMock: InvocationOnMock) =>
      val tmp = invocationOnMock.getArguments()(3).asInstanceOf[File]
      if (tmp != null) {
        outputFile.delete
        tmp.renameTo(outputFile)
      }
      null
    }.when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyLong, any(classOf[Array[Long]]), any(classOf[File]))
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
    allocator.close()
    super.afterAll()
  }

  test("write empty iterator") {
    val writer = new ColumnarShuffleWriter[Int, ColumnarBatch](
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext.taskMetrics().shuffleWriteMetrics)
    writer.write(Iterator.empty)
    writer.stop( /* success = */ true)

    assert(writer.getPartitionLengths.sum === 0)
    assert(outputFile.exists())
    assert(outputFile.length() === 0)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === 0)
    assert(shuffleWriteMetrics.recordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("write empty column batch") {
    val vectorPid = new IntVector("pid", allocator)
    val vector1 = new IntVector("v1", allocator)
    val vector2 = new IntVector("v2", allocator)

    ColumnarShuffleWriterSuite.setIntVector(vectorPid)
    ColumnarShuffleWriterSuite.setIntVector(vector1)
    ColumnarShuffleWriterSuite.setIntVector(vector2)
    val cb = ColumnarShuffleWriterSuite.makeColumnarBatch(
      vectorPid.getValueCount,
      List(vectorPid, vector1, vector2))

    def records: Iterator[(Int, ColumnarBatch)] = Iterator((0, cb), (0, cb))

    val writer = new ColumnarShuffleWriter[Int, ColumnarBatch](
      blockResolver,
      shuffleHandle,
      0L, // MapId
      taskContext.taskMetrics().shuffleWriteMetrics)

    writer.write(records)
    writer.stop(success = true)
    cb.close()

    assert(writer.getPartitionLengths.sum === 0)
    assert(outputFile.exists())
    assert(outputFile.length() === 0)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === 0)
    assert(shuffleWriteMetrics.recordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("write with some empty partitions") {
    val numRows = 4
    val vector1 = new IntVector("v1", allocator)
    val vector2 = new IntVector("v2", allocator)
    ColumnarShuffleWriterSuite.setIntVector(vector1, null, null, null, null)
    ColumnarShuffleWriterSuite.setIntVector(vector2, 100, 100, null, null)
    val cb = ColumnarShuffleWriterSuite.makeColumnarBatch(numRows, List(vector1, vector2))

    def records: Iterator[(Int, ColumnarBatch)] = Iterator((0, cb), (0, cb))

    val writer = new ColumnarShuffleWriter[Int, ColumnarBatch](
      blockResolver,
      shuffleHandle,
      0L, // MapId
      taskContext.taskMetrics().shuffleWriteMetrics)

    writer.write(records)
    writer.stop(success = true)

    assert(writer.getPartitionLengths.sum === outputFile.length())
    assert(writer.getPartitionLengths.count(_ == 0L) === 3) // should be (numPartitions - 2 * numRows) zero length files

    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === outputFile.length())
    assert(shuffleWriteMetrics.recordsWritten === records.length)

    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)

    val bytes = Files.readAllBytes(outputFile.toPath)
    val reader = new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(bytes), allocator)
    try {
      val schema = reader.getVectorSchemaRoot.getSchema
      assert(schema.getFields == this.schema.getFields)
    } finally {
      reader.close()
      cb.close()
    }
  }
}

object ColumnarShuffleWriterSuite {

  def setIntVector(vector: IntVector, values: Integer*): Unit = {
    val length = values.length
    vector.allocateNew(length)
    (0 until length).foreach { i =>
      if (values(i) != null) {
        vector.set(i, values(i).asInstanceOf[Int])
      }
    }
    vector.setValueCount(length)
  }

  def makeColumnarBatch(capacity: Int, vectors: List[FieldVector]): ColumnarBatch = {
    val columnVectors = ArrowWritableColumnVector.loadColumns(capacity, vectors.asJava)
    new ColumnarBatch(columnVectors.toArray, capacity)
  }

}
