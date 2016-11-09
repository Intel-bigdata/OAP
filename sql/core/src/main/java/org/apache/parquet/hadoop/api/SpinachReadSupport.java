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
package org.apache.parquet.hadoop.api;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * Abstraction used by the {@link org.apache.parquet.hadoop.ParquetInputFormat} to materialize records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
public abstract class SpinachReadSupport<T> {

    public static final String PARQUET_READ_FROM_FILE_SCHEMA = "parquet.read.file.schema";

    /**
     * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in
     * the front end
     *
     * @param configuration the job configuration
     * @param keyValueMetaData the app specific metadata from the file
     * @param fileSchema the schema of the file
     * @return the readContext that defines how to read the file
     *
     * @deprecated override {@link SpinachReadSupport#init(InitContext)} instead
     */
    @Deprecated
    public SpinachReadContext init(Configuration configuration, Map<String, String> keyValueMetaData,
            MessageType fileSchema) {
        throw new UnsupportedOperationException("Override init(InitContext)");
    }

    /**
     * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in
     * the front end
     *
     * @param context the initialisation context
     * @return the readContext that defines how to read the file
     */
    @SuppressWarnings("deprecation")
    public SpinachReadContext init(InitContext context) {
        return init(context.getConfiguration(), context.getMergedKeyValueMetaData(), context.getFileSchema());
    }

    /**
     * called in
     * {@link org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)}
     * in the back end the returned RecordMaterializer will materialize the records and add them to the destination
     *
     * @param configuration the job configuration
     * @param keyValueMetaData the app specific metadata from the file
     * @param fileSchema the schema of the file
     * @param readContext returned by the init method
     * @return the recordMaterializer that will materialize the records
     */
    public abstract RecordMaterializer<T> prepareForRead(Configuration configuration,
            Map<String, String> keyValueMetaData, MessageType fileSchema, SpinachReadContext readContext);
    
    public static class SpinachReadContext {
        private final MessageType readFromFileSchema;
        private final ReadContext readContext;

        public SpinachReadContext(MessageType readFromFileSchema, ReadContext readContext) {
            this.readFromFileSchema = readFromFileSchema;
            this.readContext = readContext;
        }

        /**
         * @return the schema of the file
         */
        public MessageType getRequestedSchema() {
            return readContext.getRequestedSchema();
        }

        public MessageType getReadFromFileSchema() {
            return readFromFileSchema;
        }

        /**
         * @return metadata specific to the ReadSupport implementation
         */
        public Map<String, String> getReadSupportMetadata() {
            return readContext.getReadSupportMetadata();
        }

    }
}
