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

public class ShuffleSplitterJniWrapper {

  public ShuffleSplitterJniWrapper() throws IOException {
    JniUtils.getInstance();
  }

  /**
   * Construct native splitter for shuffled RecordBatch over
   *
   * @param schemaBuf serialized arrow schema
   * @param bufferSize size of native buffers hold by each partition writer
   * @return native splitter instance id if created successfully.
   * @throws RuntimeException
   */
  public native long make(byte[] schemaBuf, long bufferSize, String localDirs)
      throws RuntimeException;

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * split according to the first column as partition id. During splitting, the data in native
   * buffers will be write to disk when the buffers are full.
   *
   * @param splitterId
   * @param numRows Rows per batch
   * @param bufAddrs Addresses of buffers
   * @param bufSizes Sizes of buffers
   * @throws RuntimeException
   */
  public native void split(long splitterId, int numRows, long[] bufAddrs, long[] bufSizes)
      throws RuntimeException;

  /**
   * Write the data remained in the buffers hold by native splitter to each partition's temporary
   * file. And stop processing splitting
   *
   * @param splitterId
   * @throws RuntimeException
   */
  public native void stop(long splitterId) throws RuntimeException;

  /**
   * Set the output buffer for each partition. Splitter will maintain one buffer for each partition
   * id occurred, and write data to file when buffer is full. Default buffer size will be set to
   * 4096 rows.
   *
   * @param splitterId
   * @param bufferSize In row, not bytes. Default buffer size will be set to 4096 rows.
   */
  public native void setPartitionBufferSize(long splitterId, long bufferSize);

  /**
   * Set compression codec for splitter's output. Default will be uncompressed.
   *
   * @param splitterId
   * @param codec "lz4", "zstd", "uncompressed"
   */
  public native void setCompressionCodec(long splitterId, String codec);

  /**
   * Get all files information created by the splitter. Used by the {@link
   * org.apache.spark.shuffle.ColumnarShuffleWriter} These files are temporarily existed and will be
   * deleted after the combination.
   *
   * @param splitterId
   * @return an array of all files information
   */
  public native PartitionFileInfo[] getPartitionFileInfo(long splitterId);

  /**
   * Get the total bytes written to disk.
   *
   * @param splitterId
   * @return
   */
  public native long getTotalBytesWritten(long splitterId);

  /**
   * Release resources associated with designated splitter instance.
   *
   * @param splitterId of the splitter instance.
   */
  public native void close(long splitterId);
}
