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

public class SkippableVectorizedRleValuesReader extends VectorizedRleValuesReader
    implements SkippableVectorizedValuesReader {

  private static final String UNSUPPORTED_OP_MSG = "only skipIntegers is valid.";

  public SkippableVectorizedRleValuesReader() {
    super();
  }

  public SkippableVectorizedRleValuesReader(int bitWidth) {
    super(bitWidth);
  }

  public void skipIntegers(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipIntegers(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipInteger();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipBooleans(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipBooleans(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipBoolean();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipBytes(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipBytes(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipByte();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipShorts(int total, int level, SkippableVectorizedValuesReader data) {
    skipIntegers(total, level, data);
  }

  public void skipLongs(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipLongs(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipLong();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipFloats(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipFloats(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipFloat();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipDoubles(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipDoubles(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipDouble();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }


  public void skipBinarys(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipBinary(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipBinary(1);
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void skipBoolean() {
    this.skipInteger();
  }

  @Override
  public void skipInteger() {
    if (this.currentCount == 0) { this.readNextGroup(); }

    this.currentCount--;
    switch (mode) {
      case RLE:
        break;
      case PACKED:
        currentBufferIdx++;
        break;
      default:
        throw new RuntimeException("Unreachable");
    }
  }

  @Override
  public void skipIntegers(int total) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          break;
        case PACKED:
          currentBufferIdx += n;
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void skipByte() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBooleans(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBytes(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipLongs(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipFloats(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipDoubles(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBinary(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipLong() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipFloat() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipDouble() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBinaryByLen(int len) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }
}
