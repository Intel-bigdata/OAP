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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.parquet.io.ParquetDecodingException;

public class SkippableVectorizedPlainValuesReader extends VectorizedPlainValuesReader
    implements SkippableVectorizedValuesReader {

  @Override
  public void skipBooleans(int total) {
    for (int i = 0; i < total; i++) {
      skipBoolean();
    }
  }

  @Override
  public void skipIntegers(int total) {
    skipFully(4L * total);
  }

  @Override
  public void skipLongs(int total) {
    skipFully(8L * total);
  }

  @Override
  public void skipFloats(int total) {
    skipFully(4L * total);
  }

  @Override
  public void skipDoubles(int total) {
    skipFully(8L * total);
  }

  @Override
  public void skipBytes(int total) {
    skipFully(4L * total);
  }

  @Override
  public void skipBoolean() {
    if (bitOffset == 0) {
      try {
        currentByte = (byte) in.read();
      } catch (IOException e) {
        throw new ParquetDecodingException("Failed to read a byte", e);
      }
    }
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
    }
  }

  @Override
  public void skipInteger() {
    skipFully(4L);
  }

  @Override
  public void skipLong() {
    skipFully(8L);
  }

  @Override
  public void skipByte() {
    skipInteger();
  }

  @Override
  public void skipFloat() {
    skipFully(4L);
  }

  @Override
  public void skipDouble() {
    skipFully(8L);
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      int length = readInteger();
      skipFully(length);
    }
  }

  @Override
  public void skipBinaryByLen(int len) {
    skipFully(len);
  }
  private static final String ERR_MESSAGE = "Failed to skip {0} bytes";

  private void skipFully(long length) {
    try {
      in.skipFully(length);
    } catch (IOException e) {
      throw  new ParquetDecodingException(MessageFormat.format(ERR_MESSAGE, length), e);
    }
  }
}
