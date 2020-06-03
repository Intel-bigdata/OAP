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

package com.intel.sparkColumnarPlugin.vectorized;

import io.netty.buffer.ArrowBuf;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

public class ExpressionEvaluator implements AutoCloseable {

  private long nativeHandler = 0;
  private ExpressionEvaluatorJniWrapper jniWrapper;

  /** Wrapper for native API. */
  public ExpressionEvaluator() throws IOException {
    jniWrapper = new ExpressionEvaluatorJniWrapper();
  }

  /** Convert ExpressionTree into native function. */
  public void build(Schema schema, List<ExpressionTree> exprs)
      throws RuntimeException, IOException, GandivaException {
    nativeHandler = jniWrapper.nativeBuild(getSchemaBytesBuf(schema), getExprListBytesBuf(exprs), null, false);
  }

  /** Convert ExpressionTree into native function. */
  public void build(Schema schema, List<ExpressionTree> exprs, boolean finishReturn)
      throws RuntimeException, IOException, GandivaException {
    nativeHandler = jniWrapper.nativeBuild(getSchemaBytesBuf(schema), getExprListBytesBuf(exprs), null, finishReturn);
  }

  /** Convert ExpressionTree into native function. */
  public void build(Schema schema, List<ExpressionTree> exprs, Schema resSchema)
      throws RuntimeException, IOException, GandivaException {
    nativeHandler = jniWrapper.nativeBuild(getSchemaBytesBuf(schema), getExprListBytesBuf(exprs), getSchemaBytesBuf(resSchema), false);
  }

  /** Convert ExpressionTree into native function. */
  public void build(Schema schema, List<ExpressionTree> exprs, Schema resSchema, boolean finishReturn)
      throws RuntimeException, IOException, GandivaException {
    nativeHandler = jniWrapper.nativeBuild(getSchemaBytesBuf(schema), getExprListBytesBuf(exprs), getSchemaBytesBuf(resSchema), finishReturn);
  }

  /** Convert ExpressionTree into native function. */
  public void build(Schema schema, List<ExpressionTree> exprs, List<ExpressionTree> finish_exprs)
      throws RuntimeException, IOException, GandivaException {
    nativeHandler = jniWrapper.nativeBuildWithFinish(getSchemaBytesBuf(schema), getExprListBytesBuf(exprs), getExprListBytesBuf(finish_exprs));
  }

  /** Set result Schema in some special cases */
  public void setReturnFields(Schema schema) throws RuntimeException, IOException, GandivaException {
    jniWrapper.nativeSetReturnFields(nativeHandler, getSchemaBytesBuf(schema));
  }

  /** Evaluate input data using builded native function, and output as recordBatch. */
  public ArrowRecordBatch[] evaluate(ArrowRecordBatch recordBatch)
      throws RuntimeException, IOException {
    return evaluate(recordBatch, null);
  }

  /** Evaluate input data using builded native function, and output as recordBatch. */
  public ArrowRecordBatch[] evaluate(ArrowRecordBatch recordBatch, SelectionVectorInt16 selectionVector)
      throws RuntimeException, IOException {
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

    ArrowRecordBatchBuilder[] resRecordBatchBuilderList;
    if (selectionVector != null) {
      int selectionVectorRecordCount = selectionVector.getRecordCount();
      long selectionVectorAddr = selectionVector.getBuffer().memoryAddress();
      long selectionVectorSize = selectionVector.getBuffer().capacity();
      resRecordBatchBuilderList =
        jniWrapper.nativeEvaluateWithSelection(nativeHandler, recordBatch.getLength(), bufAddrs, bufSizes,
            selectionVectorRecordCount, selectionVectorAddr, selectionVectorSize);
    } else {
      resRecordBatchBuilderList =
        jniWrapper.nativeEvaluate(nativeHandler, recordBatch.getLength(), bufAddrs, bufSizes);
    }
    ArrowRecordBatch[] recordBatchList = new ArrowRecordBatch[resRecordBatchBuilderList.length];
    for (int i = 0; i < resRecordBatchBuilderList.length; i++) {
      if (resRecordBatchBuilderList[i] == null) {
        recordBatchList[i] = null;
        break;
      }
      ArrowRecordBatchBuilderImpl resRecordBatchBuilderImpl =
          new ArrowRecordBatchBuilderImpl(resRecordBatchBuilderList[i]);
      recordBatchList[i] = resRecordBatchBuilderImpl.build();
    }
    return recordBatchList;
  }

  /** Evaluate input data using builded native function, and output as recordBatch. */
  public void SetMember(ArrowRecordBatch recordBatch)
      throws RuntimeException, IOException {
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

    jniWrapper.nativeSetMember(nativeHandler, recordBatch.getLength(), bufAddrs, bufSizes);
  }

  public ArrowRecordBatch[] finish() throws RuntimeException, IOException {
    ArrowRecordBatchBuilder[] resRecordBatchBuilderList = jniWrapper.nativeFinish(nativeHandler);
    ArrowRecordBatch[] recordBatchList = new ArrowRecordBatch[resRecordBatchBuilderList.length];
    for (int i = 0; i < resRecordBatchBuilderList.length; i++) {
      if (resRecordBatchBuilderList[i] == null) {
        recordBatchList[i] = null;
        break;
      }
      ArrowRecordBatchBuilderImpl resRecordBatchBuilderImpl =
          new ArrowRecordBatchBuilderImpl(resRecordBatchBuilderList[i]);
      recordBatchList[i] = resRecordBatchBuilderImpl.build();
    }
    return recordBatchList;
  }

  public BatchIterator finishByIterator() throws RuntimeException, IOException {
    long batchIteratorInstance = jniWrapper.nativeFinishByIterator(nativeHandler);
    return new BatchIterator(batchIteratorInstance);
  }

  public void setDependency(BatchIterator child) throws RuntimeException, IOException {
    jniWrapper.nativeSetDependency(nativeHandler, child.getInstanceId(), -1);
  }

  public void setDependency(BatchIterator child, int index) throws RuntimeException, IOException {
    jniWrapper.nativeSetDependency(nativeHandler, child.getInstanceId(), index);
  }

  @Override
  public void close() {
    jniWrapper.nativeClose(nativeHandler);
  }

  byte[] getSchemaBytesBuf(Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    return out.toByteArray();
  }

  byte[] getExprListBytesBuf(List<ExpressionTree> exprs) throws GandivaException {
    GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
    for (ExpressionTree expr : exprs) {
      builder.addExprs(expr.toProtobuf());
    }
    return builder.build().toByteArray();
  }
}
