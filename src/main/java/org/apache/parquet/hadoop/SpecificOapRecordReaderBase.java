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
package org.apache.parquet.hadoop;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.utils.Collections3;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.execution.datasources.oap.io.OapReadSupportImpl;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;

public abstract class SpecificOapRecordReaderBase<T> implements RecordReader<T> {

    /**
     * From SpecificParquetRecordReaderBase.
     */
    protected Path file;
    protected MessageType fileSchema;
    protected MessageType requestedSchema;
    protected StructType sparkSchema;

    /**
     * The total number of rows this RecordReader will eventually read. The sum of the
     * rows of all the row groups.
     */
    protected long totalRowCount;
    protected ParquetFileReader reader;

    /**
     * SpecificOapRecordReaderBase need
     * configuration & footer use by initialize method,
     * not belong to SpecificParquetRecordReaderBase
     */
    protected Configuration configuration;
    protected ParquetMetadata footer;

    /**
     * SpecificOapRecordReaderBase init method,
     * needn't taskAttemptContext & inputSplit
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize() throws IOException, InterruptedException {
        if(this.footer == null){
            footer = readFooter(configuration, file, NO_FILTER);
        }
        this.fileSchema = footer.getFileMetaData().getSchema();

        Map<String, String> fileMetadata = footer.getFileMetaData().getKeyValueMetaData();
        ReadSupport.ReadContext readContext = new OapReadSupportImpl().init(new InitContext(
                configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema));
        this.requestedSchema = readContext.getRequestedSchema();
        String sparkRequestedSchemaString =
                configuration.get(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA());
        this.sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
        this.reader = ParquetFileReader.open(configuration, file,footer);
        this.reader.filterRowGroups(getFilter(configuration));
        this.reader.setRequestedSchema(requestedSchema);
        for (BlockMetaData block : this.reader.getRowGroups()) {
            this.totalRowCount += block.getRowCount();
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
