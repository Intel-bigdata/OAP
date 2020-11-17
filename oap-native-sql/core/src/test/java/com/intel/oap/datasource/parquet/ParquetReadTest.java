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

package com.intel.oap.datasource.parquet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.netty.buffer.ArrowBuf;

public class ParquetReadTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  //@Test
  public void testParquetRead() throws Exception {
    File testFile = testFolder.newFile("_tmpfile_ParquetReadTest");
    // String path = testFile.getAbsolutePath();
    String path =
        "hdfs://sr602:9000/part-00000-d648dd34-c9d2-4fe9-87f2-770ef3551442-c000.snappy.parquet?user=root&replication=1";

    int numColumns = 0;
    int[] rowGroupIndices = {0};
    int[] columnIndices = new int[numColumns];

    Schema schema = new Schema(
        asList(field("n_nationkey", new Int(64, true)), field("n_name", new Utf8()),
            field("n_regionkey", new Int(64, true)), field("n_comment", new Utf8())));

    ParquetReader reader =
        new ParquetReader(path, rowGroupIndices, columnIndices, 16, allocator, "");

    Schema readedSchema = reader.getSchema();
    for (int i = 0; i < readedSchema.getFields().size(); i++) {
      assertEquals(
          schema.getFields().get(i).getName(), readedSchema.getFields().get(i).getName());
    }

    VectorSchemaRoot actualSchemaRoot = VectorSchemaRoot.create(readedSchema, allocator);
    reader.readNextVectors(actualSchemaRoot);

    System.out.println(actualSchemaRoot.getRowCount());
    assertEquals(actualSchemaRoot.getRowCount(), 16);

    List<FieldVector> fields = actualSchemaRoot.getFieldVectors();
    for (int i = 0; i < fields.size(); i++) {
      System.out.println(fields.get(i).toString());
    }
    actualSchemaRoot.close();
    reader.close();
    testFile.delete();
  }

  private static Field field(
      String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, new FieldType(nullable, type, null, null), asList(children));
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  private ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  private ArrowRecordBatch createArrowRecordBatch(VectorSchemaRoot root) {
    List<ArrowFieldNode> fieldNodes = new ArrayList<ArrowFieldNode>();
    List<ArrowBuf> inputData = new ArrayList<ArrowBuf>();
    int numRowsInBatch = root.getRowCount();
    for (FieldVector inputVector : root.getFieldVectors()) {
      fieldNodes.add(new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount()));
      inputData.add(inputVector.getValidityBuffer());
      inputData.add(inputVector.getDataBuffer());
    }
    return new ArrowRecordBatch(numRowsInBatch, fieldNodes, inputData);
  }

  private void releaseArrowRecordBatch(ArrowRecordBatch recordBatch) {
    recordBatch.close();
  }
}
