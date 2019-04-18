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
    try {
      in.skipFully(4 * total);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 4 * total + "bytes", e);
    }
  }

  @Override
  public void skipLongs(int total) {
    try {
      in.skipFully(8 * total);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 4 * total + "bytes", e);
    }
  }

  @Override
  public void skipFloats(int total) {
    try {
      in.skipFully(4 * total);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 4 * total + "bytes", e);
    }
  }

  @Override
  public void skipDoubles(int total) {
    try {
      in.skipFully(8 * total);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 4 * total + "bytes", e);
    }
  }

  @Override
  public void skipBytes(int total) {
    try {
      in.skipFully(4 * total);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 4 * total + "bytes", e);
    }
  }

  @Override
  public void skipBoolean() {
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
      try {
        in.skipFully(1);
      } catch (IOException e) {
        throw new ParquetDecodingException("Failed to skip 1 bytes", e);
      }
    }
  }

  @Override
  public void skipInteger() {
    try {
      in.skipFully(4);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 4 + "bytes", e);
    }
  }

  @Override
  public void skipLong() {
    try {
      in.skipFully(8);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 8 + "bytes", e);
    }
  }

  @Override
  public void skipByte() {
    skipInteger();
  }

  @Override
  public void skipFloat() {
    try {
      in.skipFully(4);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 4 + "bytes", e);
    }
  }

  @Override
  public void skipDouble() {
    try {
      in.skipFully(8);
    } catch (IOException e) {
      throw  new ParquetDecodingException("Failed to skip" + 8 + "bytes", e);
    }
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      int length = readInteger();
      try {
        in.skipFully(length);
      } catch (IOException e) {
        throw new ParquetDecodingException("Failed to skip " + length + " bytes", e);
      }
    }
  }

  @Override
  public void skipBinaryByLen(int len) {
    try {
      in.skipFully(len);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to skip " + len + " bytes", e);
    }
  }
}
