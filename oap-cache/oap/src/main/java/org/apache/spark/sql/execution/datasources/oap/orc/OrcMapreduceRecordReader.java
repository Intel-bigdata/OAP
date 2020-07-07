/**
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
package org.apache.spark.sql.execution.datasources.oap.orc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.*;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderBinaryCacheImpl;
import org.apache.orc.impl.RecordReaderBinaryCacheUtils;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import org.apache.spark.sql.internal.oap.OapConf$;

/**
 * This record reader is a copy of OrcMapreduceRecordReader with minor changes
 * to be able to have a child class OapIndexOrcMapreduceRecordReader.
 * This OrcMapreduceRecordReader implements the org.apache.hadoop.mapreduce API.
 * It is in the org.apache.orc.mapreduce package to share implementation with
 * the mapreduce API record reader.
 * @param <V> the root type of the file
 */
public class OrcMapreduceRecordReader<V extends WritableComparable>
    implements org.apache.spark.sql.execution.datasources.RecordReader<V> {

  protected final V row;
  protected final TypeDescription schema;
  protected final RecordReader batchReader;
  protected final VectorizedRowBatch batch;
  protected int rowInBatch;

  public OrcMapreduceRecordReader(Path file, Configuration conf)
      throws IOException {
    FileSystem fileSystem = file.getFileSystem(conf);
    long length = fileSystem.getFileStatus(file).getLen();

    Reader fileReader = OrcFile.createReader(file,
        OrcFile.readerOptions(conf)
            .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf)));
    Reader.Options options = org.apache.orc.mapred.OrcInputFormat.buildOptions(conf,
        fileReader, 0, length);

    boolean binaryCacheEnabled = conf
      .getBoolean(OapConf$.MODULE$.OAP_ORC_BINARY_DATA_CACHE_ENABLED().key(), false);
    if (binaryCacheEnabled) {
      Boolean zeroCopy = options.getUseZeroCopy();
      if (zeroCopy == null) {
        zeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(conf);
      }
      DataReader dataReader = RecordReaderBinaryCacheUtils.createBinaryCacheDataReader(
              DataReaderProperties.builder()
                      .withBufferSize(fileReader.getCompressionSize())
                      .withCompression(fileReader.getCompressionKind())
                      .withFileSystem(fileSystem)
                      .withPath(file)
                      .withTypeCount(fileReader.getTypes().size())
                      .withZeroCopy(zeroCopy)
                      .withMaxDiskRangeChunkLimit(OrcConf
                        .ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(conf))
                      .build());
      options.dataReader(dataReader);

      this.batchReader = new RecordReaderBinaryCacheImpl((ReaderImpl)fileReader, options);

    } else {
      this.batchReader = fileReader.rows(options);
    }

    if (options.getSchema() == null) {
      schema = fileReader.getSchema();
    } else {
      schema = options.getSchema();
    }
    this.batch = schema.createRowBatch();
    rowInBatch = 0;
    this.row = (V) OrcStruct.createValue(schema);
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   * @throws IOException
   */
  boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      rowInBatch = 0;
      return batchReader.nextBatch(batch);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    batchReader.close();
  }

  @Override
  public void initialize() throws IOException, InterruptedException {
    // nothing required
  }

  // Below common code is extracted from nextKeyValue method.
  // The child class IndexedOrcMapreduceRecordReader will use it as well.
  protected void readNextRow() {
    if (schema.getCategory() == TypeDescription.Category.STRUCT) {
      OrcStruct result = (OrcStruct) row;
      List<TypeDescription> children = schema.getChildren();
      int numberOfChildren = children.size();
      for(int i=0; i < numberOfChildren; ++i) {
        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], rowInBatch,
            children.get(i), result.getFieldValue(i)));
      }
    } else {
      OrcMapredRecordReader.nextValue(batch.cols[0], rowInBatch, schema, row);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!ensureBatch()) {
      return false;
    }
    readNextRow();
    rowInBatch += 1;
    return true;
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return row;
  }
}
