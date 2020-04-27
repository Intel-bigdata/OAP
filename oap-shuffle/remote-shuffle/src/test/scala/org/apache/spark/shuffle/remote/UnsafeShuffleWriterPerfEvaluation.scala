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

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.shuffle.sort.{RemoteUnsafeShuffleWriter, SerializedShuffleHandle, UnsafeShuffleWriter}

object UnsafeShuffleWriterPerfEvaluation extends ShuffleWriterPerfEvaluationBase {

  private val shuffleHandle: SerializedShuffleHandle[Int, ByteBuffer] =
    new SerializedShuffleHandle[Int, ByteBuffer](0, 0, this.dependency)

  def getRemoteWriter(mapId: Int, conf: SparkConf): RemoteUnsafeShuffleWriter[Int, ByteBuffer] = {

    val taskMemoryManager = setup()

    new RemoteUnsafeShuffleWriter[Int, ByteBuffer](
      blockManager,
      blockResolverRemote,
      taskMemoryManager,
      shuffleHandle,
      mapId,
      taskContext.get(),
      conf)
  }

}
