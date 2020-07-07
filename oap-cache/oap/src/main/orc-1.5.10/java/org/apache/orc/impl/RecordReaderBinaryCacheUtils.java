/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.orc.DataReader;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager;
import org.apache.spark.sql.execution.datasources.oap.filecache.OrcBinaryFiberId;
import org.apache.spark.sql.execution.datasources.oap.io.DataFile;
import org.apache.spark.sql.execution.datasources.oap.io.OrcDataFile;
import org.apache.spark.sql.oap.OapRuntime$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RecordReaderBinaryCacheUtils extends RecordReaderUtils {

  protected static class BinaryCacheDataReader extends DefaultDataReader {
    private DataFile dataFile;

    private BinaryCacheDataReader(DataReaderProperties properties) {
      super(properties);
      this.dataFile = new OrcDataFile(path.toUri().toString(),
              new StructType(), new Configuration());
    }

    public DiskRangeList readFileColumnData(
            DiskRangeList range, long baseOffset, boolean doForceDirect) {
      return RecordReaderBinaryCacheUtils.readColumnRanges(file, path, baseOffset, range, doForceDirect, dataFile);
    }
  }

  public static DataReader createBinaryCacheDataReader(DataReaderProperties properties) {
    return new BinaryCacheDataReader(properties);
  }

  /**
   * Read the list of ranges from the file.
   * @param file the file to read
   * @param base the base of the stripe
   * @param range the disk ranges within the stripe to read
   * @return the bytes read for each disk range, which is the same length as
   *    ranges
   * @throws IOException
   */
  static DiskRangeList readColumnRanges(FSDataInputStream file,
                                        Path path,
                                        long base,
                                        DiskRangeList range,
                                        boolean doForceDirect,
                                        DataFile dataFile) {
    if (range == null) return null;
    DiskRangeList prev = range.prev;
    if (prev == null) {
      prev = new DiskRangeList.MutateHelper(range);
    }
    while (range != null) {
      if (range.hasData()) {
        range = range.next;
        continue;
      }
      int len = (int) (range.getEnd() - range.getOffset());
      long off = range.getOffset();
      byte[] buffer = new byte[len];

      FiberCacheManager cacheManager = OapRuntime$.MODULE$.getOrCreate().fiberCacheManager();
      FiberCache fiberCache = null;
      OrcBinaryFiberId fiberId = null;
      try {
        ColumnDiskRangeList columnRange = (ColumnDiskRangeList)range;
        fiberId = new OrcBinaryFiberId(dataFile, columnRange.columnId, columnRange.currentStripe);
        fiberId.withLoadCacheParameters(file, base + off, len);
        fiberCache = cacheManager.get(fiberId);
        long fiberOffset = fiberCache.getBaseOffset();
        Platform.copyMemory(null, fiberOffset, buffer, Platform.BYTE_ARRAY_OFFSET, len);
      } finally {
        if (fiberCache != null) {
          fiberCache.release();
        }
        if (fiberId != null) {
          fiberId.cleanLoadCacheParameters();
        }
      }
      ByteBuffer bb = null;
      if (doForceDirect) {
        bb = ByteBuffer.allocateDirect(len);
        bb.put(buffer);
        bb.position(0);
        bb.limit(len);
      } else {
        bb = ByteBuffer.wrap(buffer);
      }
      range = range.replaceSelfWith(new BufferChunk(bb, range.getOffset()));
      range = range.next;
    }
    return prev.next;
  }

}
