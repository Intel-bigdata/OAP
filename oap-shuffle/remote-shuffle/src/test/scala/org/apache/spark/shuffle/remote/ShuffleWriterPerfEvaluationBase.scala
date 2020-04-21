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

import java.io.File
import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.mockito._
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.when

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryManager, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.serializer.{KryoSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockResolver}
import org.apache.spark.storage.{BlockManager, DiskBlockManager, TempShuffleBlockId}
import org.apache.spark.util.Utils

abstract class ShuffleWriterPerfEvaluationBase {

  protected val defaultConf = new SparkConf(loadDefaults = false)

  // This is only used in the writer constructors, so it's ok to mock
  @Mock(answer = RETURNS_SMART_NULLS) protected var dependency:
  ShuffleDependency[Int, ByteBuffer, ByteBuffer] = _
  // This is only used in the stop() function, so we can safely mock this without affecting perf
  protected val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]
  @Mock(answer = RETURNS_SMART_NULLS) protected var rpcEnv: RpcEnv = _
  @Mock(answer = RETURNS_SMART_NULLS) protected var rpcEndpointRef: RpcEndpointRef = _

  protected val serializer: Serializer = new KryoSerializer(defaultConf)
  protected val serializerManager: SerializerManager =
    new SerializerManager(serializer, defaultConf)
  protected val shuffleMetrics: TaskMetrics = new TaskMetrics
  protected var partitioner: HashPartitioner = _


  protected val tempFilesCreated: ArrayBuffer[File] = new ArrayBuffer[File]
  protected val filenameToFile: mutable.Map[String, File] = new mutable.HashMap[String, File]

  class TestDiskBlockManager(tempDir: File) extends DiskBlockManager(defaultConf, false) {
    override def getFile(filename: String): File = {
      if (filenameToFile.contains(filename)) {
        filenameToFile(filename)
      } else {
        val outputFile = File.createTempFile("shuffle", null, tempDir)
        filenameToFile(filename) = outputFile
        outputFile
      }
    }

    override def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
      var blockId = new TempShuffleBlockId(UUID.randomUUID())
      val file = getFile(blockId)
      tempFilesCreated += file
      (blockId, file)
    }
  }

  class TestBlockManager(tempDir: File, memoryManager: MemoryManager, conf: SparkConf)
    extends BlockManager("0",
    rpcEnv,
    null,
    serializerManager,
      conf,
    memoryManager,
    null,
    null,
    null,
    null,
    1) {
    override val diskBlockManager = new TestDiskBlockManager(tempDir)
    override val remoteBlockTempFileManager = null
  }

  protected var tempDir: File = _

  protected var blockManager: BlockManager = _
  protected var blockResolver: IndexShuffleBlockResolver = _
  protected var blockResolverRemote: RemoteShuffleBlockResolver = _

  protected var memoryManager: TestMemoryManager = _

  MockitoAnnotations.initMocks(this)
  when(dependency.serializer).thenReturn(serializer)
  when(dependency.shuffleId).thenReturn(0)
  when(dependency.mapSideCombine).thenReturn(false)
  when(dependency.aggregator).thenReturn(None)
  when(dependency.keyOrdering).thenReturn(None)

  when(rpcEnv.setupEndpoint(any[String], any[RpcEndpoint])).thenReturn(rpcEndpointRef)

  def globalSetup(reducers: Int, storageMasterUri: String, shuffleDir: String,
    conf: SparkConf, remoteConf: SparkConf): ShuffleBlockResolver = {
    partitioner = new HashPartitioner(reducers)
    when(dependency.partitioner).thenReturn(partitioner)


    // For vanilla Spark shuffle directory customization
    tempDir = Utils.createTempDir(shuffleDir)
    memoryManager = new TestMemoryManager(conf)
    // Infinite memory
    // memoryManager.limit(MAXIMUM_PAGE_SIZE_BYTES)

    blockManager = new TestBlockManager(tempDir, memoryManager, conf)
    blockResolver = new IndexShuffleBlockResolver(
      conf,
      blockManager)
    blockResolverRemote = new RemoteShuffleBlockResolver(remoteConf)
    blockResolverRemote
  }

  protected def setup(): TaskMemoryManager = {
    val taskContext = mock(classOf[TaskContext])
    this.taskContext.set(taskContext)

    when(taskContext.taskMetrics()).thenReturn(shuffleMetrics)

    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    when(taskContext.taskMemoryManager()).thenReturn(taskMemoryManager)
    TaskContext.setTaskContext(taskContext)
    taskMemoryManager
  }
}
