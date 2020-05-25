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

import org.apache.orc.*;
import org.apache.orc.storage.common.io.DiskRangeList;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordReaderBinaryCacheImpl extends RecordReaderImpl {

  public RecordReaderBinaryCacheImpl(ReaderImpl fileReader,
                                     Reader.Options options) throws IOException {
    super(fileReader, options);
  }

  /**
   * Read the current stripe into memory.
   *
   * @throws IOException
   */
  protected void readStripe() throws IOException {
    StripeInformation stripe = beginReadStripe();

    if (rowInStripe < rowCountInStripe) {
      readPartialDataStreams(stripe);
      reader.startStripe(streams, stripeFooter);
      // if we skipped the first row group, move the pointers forward
      if (rowInStripe != 0) {
        seekToRowEntry(reader, (int) (rowInStripe / rowIndexStride));
      }
    }
  }

  private void readPartialDataStreams(StripeInformation stripe) throws IOException {
    List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
    DiskRangeList toRead = planReadColumnData(streamList, fileIncluded);
    if (LOG.isDebugEnabled()) {
      LOG.debug("chunks = " + RecordReaderBinaryCacheUtils.stringifyDiskRanges(toRead));
    }
    bufferChunks = ((RecordReaderBinaryCacheUtils.BinaryCacheDataReader)dataReader)
            .readFileColumnData(toRead, stripe.getOffset(), false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("merge = " + RecordReaderBinaryCacheUtils.stringifyDiskRanges(bufferChunks));
    }
    createStreams(streamList, bufferChunks, fileIncluded,
            dataReader.getCompressionCodec(), bufferSize, streams);
  }


  /**
   * Plan the ranges of the file that we need to read, given the list of
   * columns in one stripe.
   *
   * @param streamList        the list of streams available
   * @param includedColumns   which columns are needed
   * @return the list of disk ranges that will be loaded
   */
  private DiskRangeList planReadColumnData(
          List<OrcProto.Stream> streamList,
          boolean[] includedColumns) {
    long offset = 0;
    Map<Integer, RecordReaderBinaryCacheImpl.ColumnDiskRange> columnDiskRangeMap = new HashMap<Integer, RecordReaderBinaryCacheImpl.ColumnDiskRange>();
    ColumnDiskRangeList.CreateColumnRangeHelper list =
            new ColumnDiskRangeList.CreateColumnRangeHelper();
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int column = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      // since stream kind is optional, first check if it exists
      if (stream.hasKind() &&
              (StreamName.getArea(streamKind) == StreamName.Area.DATA) &&
              (column < includedColumns.length && includedColumns[column])) {

        if (columnDiskRangeMap.containsKey(column)) {
          columnDiskRangeMap.get(column).length += length;
        } else {
          columnDiskRangeMap.put(column, new RecordReaderBinaryCacheImpl.ColumnDiskRange(offset, length));
        }
      }
      offset += length;
    }
    // Start from index 1, because includedColumns[0] is invalid in ORC.
    for (int columnId=1; columnId<includedColumns.length; ++columnId) {
      if (includedColumns[columnId]) {
        list.add(columnId, currentStripe, columnDiskRangeMap.get(columnId).offset,
                columnDiskRangeMap.get(columnId).offset + columnDiskRangeMap.get(columnId).length);
      }
    }
    return list.extract();
  }

  public class ColumnDiskRange {
    private long offset;
    private long length;

    ColumnDiskRange(long offset, long length) {
      this.offset = offset;
      this.length = length;
    }

    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other instanceof ColumnDiskRange) {
        ColumnDiskRange otherColumnDiskRange = (ColumnDiskRange)other;
        return otherColumnDiskRange.offset == this.offset &&
                otherColumnDiskRange.length == this.length;
      }
      return false;
    }
  }

}
