/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetReadMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetReadMetrics.class);

    private long totalTimeSpentReadingBytes;
    private long totalTimeSpentProcessingRecords;
    private long startedAssemblingCurrentBlockAt;
    private long readRowGroupStart;

    public void startRecordAssemblyTime(){
        this.startedAssemblingCurrentBlockAt = System.currentTimeMillis();
    }

    public void startReadOneRowGroup(){
        this.readRowGroupStart = System.currentTimeMillis();
    }

    public void overReadOneRowGroup(PageReadStore pages){
        long timeSpentReading = System.currentTimeMillis() - readRowGroupStart;
        totalTimeSpentReadingBytes += timeSpentReading;
        BenchmarkCounter.incrementTime(timeSpentReading);
        if (LOG.isInfoEnabled()) {
            LOG.info("block read in memory in {} ms. row count = {}",
                    timeSpentReading, pages.getRowCount());
        }
    }

    public void recordMetrics(long totalCountLoadedSoFar, int columnCount){
        totalTimeSpentProcessingRecords
                += (System.currentTimeMillis() - startedAssemblingCurrentBlockAt);
        if (LOG.isInfoEnabled()) {
            LOG.info("Assembled and processed " + totalCountLoadedSoFar + " records from "
                    + columnCount + " columns in " + totalTimeSpentProcessingRecords + " ms: "
                    + ((float) totalCountLoadedSoFar / totalTimeSpentProcessingRecords) + " rec/ms, "
                    + ((float) totalCountLoadedSoFar * columnCount / totalTimeSpentProcessingRecords)
                    + " cell/ms");
            final long totalTime = totalTimeSpentProcessingRecords + totalTimeSpentReadingBytes;
            if (totalTime != 0) {
                final long percentReading = 100 * totalTimeSpentReadingBytes / totalTime;
                final long percentProcessing = 100 * totalTimeSpentProcessingRecords / totalTime;
                LOG.info("time spent so far " + percentReading + "% reading ("
                        + totalTimeSpentReadingBytes + " ms) and " + percentProcessing
                        + "% processing (" + totalTimeSpentProcessingRecords + " ms)");
            }
        }
    }

}
