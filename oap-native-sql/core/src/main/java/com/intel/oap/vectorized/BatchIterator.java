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
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import java.util.List;
import io.netty.buffer.ArrowBuf;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

public class BatchIterator {
  private native ArrowRecordBatchBuilder nativeNext(long nativeHandler);
  private native ArrowRecordBatchBuilder nativeProcess(long nativeHandler, byte[] schemaBuf, int numRows, long[] bufAddrs, long[] bufSizes);
  private native void nativeProcessAndCacheOne(long nativeHandler, byte[] schemaBuf, int numRows, long[] bufAddrs, long[] bufSizes);
  private native ArrowRecordBatchBuilder nativeProcessWithSelection(long nativeHandler, byte[] schemaBuf, int numRows, long[] bufAddrs, long[] bufSizes,
      int selectionVectorRecordCount, long selectionVectorAddr, long selectionVectorSize);
  private native void nativeProcessAndCacheOneWithSelection(long nativeHandler, byte[] schemaBuf, int numRows, long[] bufAddrs, long[] bufSizes,
      int selectionVectorRecordCount, long selectionVectorAddr, long selectionVectorSize);

  private native void nativeClose(long nativeHandler);

  private long nativeHandler = 0;
  private boolean closed = false;

  public BatchIterator() throws IOException {
  }

  public BatchIterator(long instance_id) throws IOException {
    JniUtils.getInstance();
    nativeHandler = instance_id;
  }

  public ArrowRecordBatch next() throws IOException {
    if (nativeHandler == 0) {
      return null;
    }
    ArrowRecordBatchBuilder resRecordBatchBuilder = nativeNext(nativeHandler);
    if (resRecordBatchBuilder == null) {
      return null;
    }
    ArrowRecordBatchBuilderImpl resRecordBatchBuilderImpl =
        new ArrowRecordBatchBuilderImpl(resRecordBatchBuilder);
    return resRecordBatchBuilderImpl.build();
  }

  public ArrowRecordBatch process(Schema schema, ArrowRecordBatch recordBatch) throws IOException {
    return process(schema, recordBatch, null);
  }

  public ArrowRecordBatch process(Schema schema, ArrowRecordBatch recordBatch,
      SelectionVectorInt16 selectionVector) throws IOException {
    int num_rows = recordBatch.getLength();
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    if (nativeHandler == 0) {
      return null;
    }
    ArrowRecordBatchBuilder resRecordBatchBuilder;
    if (selectionVector != null) {
      int selectionVectorRecordCount = selectionVector.getRecordCount();
      long selectionVectorAddr = selectionVector.getBuffer().memoryAddress();
      long selectionVectorSize = selectionVector.getBuffer().capacity();
      resRecordBatchBuilder = nativeProcessWithSelection(
        nativeHandler, getSchemaBytesBuf(schema), num_rows, bufAddrs, bufSizes, 
        selectionVectorRecordCount, selectionVectorAddr, selectionVectorSize);
    } else {
      resRecordBatchBuilder = nativeProcess(nativeHandler, getSchemaBytesBuf(schema), num_rows, bufAddrs, bufSizes);
    }
    if (resRecordBatchBuilder == null) {
      return null;
    }
    ArrowRecordBatchBuilderImpl resRecordBatchBuilderImpl =
        new ArrowRecordBatchBuilderImpl(resRecordBatchBuilder);
    return resRecordBatchBuilderImpl.build();
  }

  public void processAndCacheOne(Schema schema, ArrowRecordBatch recordBatch) throws IOException {
    processAndCacheOne(schema, recordBatch, null);
  }

  public void processAndCacheOne(Schema schema, ArrowRecordBatch recordBatch, 
      SelectionVectorInt16 selectionVector) throws IOException {
    int num_rows = recordBatch.getLength();
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    if (nativeHandler == 0) {
      return;
    }
    if (selectionVector != null) {
      int selectionVectorRecordCount = selectionVector.getRecordCount();
      long selectionVectorAddr = selectionVector.getBuffer().memoryAddress();
      long selectionVectorSize = selectionVector.getBuffer().capacity();
      nativeProcessAndCacheOneWithSelection(
          nativeHandler, getSchemaBytesBuf(schema), num_rows, bufAddrs, bufSizes,
          selectionVectorRecordCount, selectionVectorAddr, selectionVectorSize);
    } else {
      nativeProcessAndCacheOne(nativeHandler, getSchemaBytesBuf(schema), num_rows, bufAddrs, bufSizes);
    }
  }

  public void close() {
    if (!closed) {
      nativeClose(nativeHandler);
      closed = true;
    }
  }

  byte[] getSchemaBytesBuf(Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    return out.toByteArray();
  }

  long getInstanceId() {
    return nativeHandler;
  }

}
