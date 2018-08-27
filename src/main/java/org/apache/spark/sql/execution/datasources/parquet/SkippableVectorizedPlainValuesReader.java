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
    offset += 4 * total;
  }

  @Override
  public void skipLongs(int total) {
    offset += 8 * total;
  }

  @Override
  public void skipFloats(int total) {
    offset += 4 * total;
  }

  @Override
  public void skipDoubles(int total) {
    offset += 8 * total;
  }

  @Override
  public void skipBytes(int total) {
    offset += 4 * total;
  }

  @Override
  public void skipBoolean() {
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
      offset++;
    }
  }

  @Override
  public void skipInteger() {
    offset += 4;
  }

  @Override
  public void skipLong() {
    offset += 8;
  }

  @Override
  public void skipByte() {
    skipInteger();
  }

  @Override
  public void skipFloat() {
    offset += 4;
  }

  @Override
  public void skipDouble() {
    offset += 8;
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      offset += len;
    }
  }

  @Override
  public void skipBinaryByLen(int len) {
    offset += len;
  }
}
