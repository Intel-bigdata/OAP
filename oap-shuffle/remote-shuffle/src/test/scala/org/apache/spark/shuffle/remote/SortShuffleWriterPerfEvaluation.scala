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

import org.mockito.Mockito.when

import org.apache.spark.Aggregator
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleWriter

object SortShuffleWriterPerfEvaluation extends ShuffleWriterPerfEvaluationBase {

  private val shuffleHandle: BaseShuffleHandle[Int, ByteBuffer, ByteBuffer] =
    new BaseShuffleHandle(
      shuffleId = 0,
      numMaps = 1,
      dependency = dependency)

  def getWriter(mapId: Int): SortShuffleWriter[Int, ByteBuffer, ByteBuffer] = {

    setup()

    new SortShuffleWriter[Int, ByteBuffer, ByteBuffer](
      blockResolver,
      shuffleHandle,
      mapId,
      taskContext.get())
  }

  def getRemoteWriter(mapId: Int): RemoteShuffleWriter[Int, ByteBuffer, ByteBuffer] = {

    setup()

    new RemoteShuffleWriter[Int, ByteBuffer, ByteBuffer](
      blockResolverRemote,
      shuffleHandle,
      mapId,
      taskContext.get())
  }

}
