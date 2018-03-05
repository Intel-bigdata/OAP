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

import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import java.io.IOException;
import java.util.Map;

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
     *
     * @param footer parquet file footer
     * @param configuration haddoop configuration
     * @param isFilterRowGroups is do filterRowGroups
     * @throws IOException
     * @throws InterruptedException
     */
    protected void initialize(ParquetMetadata footer, Configuration configuration, boolean isFilterRowGroups)
        throws IOException, InterruptedException {
      this.fileSchema = footer.getFileMetaData().getSchema();
      Map<String, String> fileMetadata = footer.getFileMetaData().getKeyValueMetaData();
      ReadSupport.ReadContext readContext = new OapReadSupportImpl().init(new InitContext(
        configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema));
      this.requestedSchema = readContext.getRequestedSchema();
      String sparkRequestedSchemaString =
        configuration.get(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA());
      this.sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
      this.reader = ParquetFileReader.open(configuration, file, footer);
      if (isFilterRowGroups) {
        this.reader.filterRowGroups(getFilter(configuration));
      }
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
