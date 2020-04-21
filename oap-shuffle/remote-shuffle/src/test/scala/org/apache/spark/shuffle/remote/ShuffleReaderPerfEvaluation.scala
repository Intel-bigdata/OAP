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
package org.apache.spark.shuffle.remote

import java.nio.ByteBuffer

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.source.Source
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.shuffle.{BaseShuffleHandle, FetchFailedException, ShuffleBlockResolver}
import org.apache.spark.storage._
import org.apache.spark.util._

object ShuffleReaderPerfEvaluation {

  private val defaultConf = new SparkConf(loadDefaults = false)

  // this is only used to retrieve the aggregator/sorters/serializers,
  // so it shouldn't affect the performance significantly
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency: ShuffleDependency[
    Int, ByteBuffer, ByteBuffer] = _
  // only used to retrieve info about the maps at the beginning, doesn't affect perf
  @Mock(answer = RETURNS_SMART_NULLS) private var mapOutputTracker: MapOutputTracker = _

  private val serializer = new KryoSerializer(defaultConf)
  private val serializerManager = new SerializerManager(serializer, defaultConf)

  private var remoteBlockResolver: ShuffleBlockResolver = _

  private var shuffleHandle: BaseShuffleHandle[Int, ByteBuffer, ByteBuffer] = _

  def globalSetup(
    mappers: Int,
    resolver: ShuffleBlockResolver,
    mapStatus: MapStatus,
    storageMasterUri: String,
    shuffleDir: String,
    conf: SparkConf,
    remoteConf: SparkConf): Unit = {

    MockitoAnnotations.initMocks(this)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)

    shuffleHandle = new BaseShuffleHandle(
      shuffleId = 0,
      numMaps = mappers,
      dependency = dependency)

    this.remoteBlockResolver = resolver

    when(mapOutputTracker.getMapSizesByExecutorId(any[Int](), any[Int](), any[Int]()))
      .thenAnswer(new Answer[Iterator[(BlockManagerId, Seq[(BlockId, Long)])]] {
        def answer(invocationOnMock: InvocationOnMock):
        Iterator[(BlockManagerId, Seq[(BlockId, Long)])] = {
          val startPartition = invocationOnMock.getArguments()(1).asInstanceOf[Int]
          val endPartition = invocationOnMock.getArguments()(2).asInstanceOf[Int]

          MapOutputTracker.convertMapStatuses(
            0, startPartition, endPartition, Array.fill(mappers)(mapStatus))
        }
      })
  }

  def getRemoteReader(
    startPartition: Int,
    endPartition: Int): RemoteShuffleReader[Int, ByteBuffer] = {

    val taskContext = new TestTaskContext
    TaskContext.setTaskContext(taskContext)

    new RemoteShuffleReader[Int, ByteBuffer](
      shuffleHandle,
      remoteBlockResolver.asInstanceOf[RemoteShuffleBlockResolver],
      startPartition,
      endPartition,
      taskContext,
      serializerManager,
      mapOutputTracker
    )
  }

  def getReader(
    startPartition: Int,
    endPartition: Int): RemoteShuffleReader[Int, ByteBuffer] = {

    val taskContext = new TestTaskContext
    TaskContext.setTaskContext(taskContext)

    new RemoteShuffleReader[Int, ByteBuffer](
      shuffleHandle,
      remoteBlockResolver.asInstanceOf[RemoteShuffleBlockResolver],
      startPartition,
      endPartition,
      taskContext,
      serializerManager,
      mapOutputTracker
    )
  }

  // We cannot mock the TaskContext because it taskMetrics() gets called at every next()
  // call on the reader, and Mockito will try to log all calls to taskMetrics(), thus OOM-ing
  // the test
  class TestTaskContext extends TaskContext {
    private val metrics: TaskMetrics = new TaskMetrics
    private val testMemManager = new TestMemoryManager(defaultConf)
    private val taskMemManager = new TaskMemoryManager(testMemManager, 0)
    // Infinite memory
    // testMemManager.limit(MAXIMUM_PAGE_SIZE_BYTES)
    override def isCompleted(): Boolean = false
    override def isInterrupted(): Boolean = false
    override def addTaskCompletionListener(listener: TaskCompletionListener):
    TaskContext = { null }
    override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = { null }
    override def stageId(): Int = 0
    override def stageAttemptNumber(): Int = 0
    override def partitionId(): Int = 0
    override def attemptNumber(): Int = 0
    override def taskAttemptId(): Long = 0
    override def getLocalProperty(key: String): String = ""
    override def taskMetrics(): TaskMetrics = metrics
    override def getMetricsSources(sourceName: String): Seq[Source] = Seq.empty
    override private[spark] def killTaskIfInterrupted(): Unit = {}
    override private[spark] def getKillReason() = None
    override private[spark] def taskMemoryManager() = taskMemManager
    override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {}
    override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {}
    override private[spark] def markInterrupted(reason: String): Unit = {}
    override private[spark] def markTaskFailed(error: Throwable): Unit = {}
    override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = {}
    override private[spark] def fetchFailed = None
    override private[spark] def getLocalProperties = { null }

    override def isRunningLocally(): Boolean = false
  }

}
