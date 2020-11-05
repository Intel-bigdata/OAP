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

package com.intel.oap.vectorized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.NativeUnderlyingMemory;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import io.netty.buffer.ArrowBuf;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;

/** ArrowRecordBatchBuilderImpl used to wrap native returned data into an ArrowRecordBatch. */
public class ArrowRecordBatchBuilderImpl {

  private int length;
  private ArrowRecordBatchBuilder recordBatchBuilder;

  /**
   * Create ArrowRecordBatchBuilderImpl instance from ArrowRecordBatchBuilder.
   *
   * @param recordBatchBuilder ArrowRecordBatchBuilder instance.
   */
  public ArrowRecordBatchBuilderImpl(ArrowRecordBatchBuilder recordBatchBuilder) {
    this.recordBatchBuilder = recordBatchBuilder;
  }

  /**
   * Build ArrowRecordBatch from ArrowRecordBatchBuilder instance.
   *
   * @throws IOException throws exception
   */
  public ArrowRecordBatch build() throws IOException {
    if (recordBatchBuilder.length == 0) {
      return null;
    }

    List<ArrowFieldNode> nodes = new ArrayList<ArrowFieldNode>();
    for (ArrowFieldNodeBuilder tmp : recordBatchBuilder.nodeBuilders) {
      nodes.add(new ArrowFieldNode(tmp.length, tmp.nullCount));
    }

    List<ArrowBuf> buffers = new ArrayList<ArrowBuf>();
    for (ArrowBufBuilder tmp : recordBatchBuilder.bufferBuilders) {
      BaseAllocator allocator = SparkMemoryUtils.arrowAllocator();
      NativeUnderlyingMemory am = new Underlying(allocator, tmp.size,
          tmp.nativeInstanceId, tmp.memoryAddress);
      BufferLedger ledger = am.associate(allocator);
      buffers.add(new ArrowBuf(ledger, null, tmp.size, tmp.memoryAddress, false));
    }
    try {
      return new ArrowRecordBatch(recordBatchBuilder.length, nodes, buffers);
    } finally {
      buffers.forEach(ArrowBuf::close);
    }
  }

  private static class Underlying extends NativeUnderlyingMemory {
    private final long nativeInstanceId;

    public Underlying(BaseAllocator accountingAllocator, int size,
                      long nativeInstanceId, long address) {
      super(accountingAllocator, size, nativeInstanceId, address);
      this.nativeInstanceId = nativeInstanceId;
    }

    @Override
    protected void release0() {
      AdaptorReferenceManager.DEFAULT.nativeRelease(nativeInstanceId);
    }
  }
}
