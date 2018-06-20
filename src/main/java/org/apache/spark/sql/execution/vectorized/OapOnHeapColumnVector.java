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
package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.oap.io.FiberUsable;
import org.apache.spark.sql.oap.OapRuntime$;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

public class OapOnHeapColumnVector extends OnHeapColumnVector implements FiberUsable {

  public OapOnHeapColumnVector(int capacity, DataType type) {
    super(capacity, type);
  }

  @Override
  public void dumpBytesToCache(long nativeAddress) {
    if (type instanceof ByteType) {
      // data: 1 byte, nulls: 1 byte
      if (dictionary == null) {
        Platform.copyMemory(byteData, Platform.BYTE_ARRAY_OFFSET, null,
                nativeAddress, capacity);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putByte(null, nativeAddress + i, getByte(i));
          }
        }
      }
      Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity, capacity);
    } else if (type instanceof BooleanType) {
      // data: 1 byte, nulls: 1 byte
      if (dictionary == null) {
        Platform.copyMemory(byteData, Platform.BYTE_ARRAY_OFFSET, null,
                nativeAddress, capacity);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putByte(null, nativeAddress + i,
                    (byte) ((getBoolean(i)) ? 1 : 0));
          }
        }
      }
      Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity, capacity);
    } else if (type instanceof ShortType) {
      // data: 2 bytes, nulls: 1 byte
      if (dictionary == null) {
        Platform.copyMemory(shortData, Platform.SHORT_ARRAY_OFFSET, null,
                nativeAddress, capacity * 2);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putShort(null, nativeAddress + i * 2, getShort(i));
          }
        }
      }
      Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 2, capacity);
    } else if (type instanceof IntegerType || type instanceof DateType) {
      // data: 4 bytes, nulls: 1 byte
      if (dictionary == null) {
        Platform.copyMemory(intData, Platform.INT_ARRAY_OFFSET, null,
                nativeAddress, capacity * 4);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putInt(null, nativeAddress + i * 4, getInt(i));
          }
        }
      }
      Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 4, capacity);
    } else if (type instanceof FloatType) {
      // data: 4 bytes, nulls: 1 byte
      if (dictionary == null) {
        Platform.copyMemory(floatData, Platform.FLOAT_ARRAY_OFFSET, null,
                nativeAddress, capacity * 4);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putFloat(null, nativeAddress + i * 4, getFloat(i));
          }
        }
      }
      Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 4, capacity);
    } else if (type instanceof LongType) {
      // data: 8 bytes, nulls: 1 byte
      if (dictionary == null) {
        Platform.copyMemory(longData, Platform.LONG_ARRAY_OFFSET, null,
                nativeAddress, capacity * 8);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putLong(null, nativeAddress + i * 8, getLong(i));
          }
        }
      }
      Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 8, capacity);
    } else if (type instanceof DoubleType) {
      // data: 8 bytes, nulls: 1 byte
      if (dictionary == null) {
        Platform.copyMemory(doubleData, Platform.DOUBLE_ARRAY_OFFSET, null,
                nativeAddress, capacity * 8);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putDouble(null, nativeAddress + i * 8, getDouble(i));
          }
        }
      }
      Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 8, capacity);
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
  }

  @Override
  public FiberCache dumpBytesToCache() {
    byte[] dataBytes = null;
    if (type instanceof BinaryType) {
      // lengthData: 4 bytes, offsetData: 4 bytes, nulls: 1 byte,
      // child.data: childColumns[0].elementsAppended bytes.
      if (dictionary == null) {
        dataBytes = new byte[capacity * (4 + 4 + 1) + childColumns[0].elementsAppended];
        Platform.copyMemory(arrayLengths, Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 4);
        Platform.copyMemory(arrayOffsets, Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 4, capacity * 4);
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = ((OnHeapColumnVector)childColumns[0]).byteData;
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                childColumns[0].elementsAppended);
      } else {
        byte[] tempBytes = new byte[capacity * (4 + 4)];
        int offset = 0;
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            byte[] bytes = null;
            bytes = getBinary(i);
            Platform.putInt(tempBytes, Platform.INT_ARRAY_OFFSET + i * 4, bytes.length);
            Platform.putInt(tempBytes, Platform.INT_ARRAY_OFFSET + capacity * 4 + i * 4,
                    offset);
            arrayData().appendBytes(bytes.length, bytes, 0);
            offset += bytes.length;
          }
        }
        dataBytes = new byte[capacity * (4 + 4 + 1) + childColumns[0].elementsAppended];
        Platform.copyMemory(tempBytes, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 8);
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET , dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = ((OnHeapColumnVector)childColumns[0]).byteData;
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                childColumns[0].elementsAppended);
      }
    } else if (type instanceof StringType) {
      // lengthData: 4 bytes, offsetData: 4 bytes, nulls: 1 byte,
      // child.data: childColumns[0].elementsAppended bytes.
      if (dictionary == null) {
        dataBytes = new byte[capacity * (4 + 4 + 1) + childColumns[0].elementsAppended];
        Platform.copyMemory(arrayLengths, Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 4);
        Platform.copyMemory(arrayOffsets, Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 4, capacity * 4);
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = ((OnHeapColumnVector)childColumns[0]).byteData;
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                childColumns[0].elementsAppended);
      } else {
        byte[] tempBytes = new byte[capacity * (4 + 4)];
        int offset = 0;
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            byte[] bytes = null;
            bytes = getUTF8String(i).getBytes();
            Platform.putInt(tempBytes, Platform.INT_ARRAY_OFFSET + i * 4, bytes.length);
            Platform.putInt(tempBytes, Platform.INT_ARRAY_OFFSET + capacity * 4 + i * 4,
                    offset);
            arrayData().appendBytes(bytes.length, bytes, 0);
            offset += bytes.length;
          }
        }
        dataBytes = new byte[capacity * (4 + 4 + 1) + childColumns[0].elementsAppended];
        Platform.copyMemory(tempBytes, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 8);
        Platform.copyMemory(nulls, Platform.BYTE_ARRAY_OFFSET , dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = ((OnHeapColumnVector)childColumns[0]).byteData;
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                childColumns[0].elementsAppended);
      }
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    return OapRuntime$.MODULE$.getOrCreate().memoryManager().toDataFiberCache(dataBytes);
  }

  @Override
  public void loadBytesFromCache(long nativeAddress) {
    if (type instanceof ByteType || type instanceof BooleanType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, byteData,
              Platform.BYTE_ARRAY_OFFSET, capacity);
      Platform.copyMemory(null, nativeAddress + capacity,
              nulls, Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof ShortType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, shortData,
              Platform.SHORT_ARRAY_OFFSET, capacity * 2);
      Platform.copyMemory(null, nativeAddress + capacity * 2,
              nulls, Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof IntegerType || type instanceof DateType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, intData,
              Platform.INT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 4,
              nulls, Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof FloatType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, floatData,
              Platform.FLOAT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 4,
              nulls, Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof LongType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, longData,
              Platform.LONG_ARRAY_OFFSET, capacity * 8);
      Platform.copyMemory(null, nativeAddress + capacity * 8,
              nulls, Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof DoubleType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, doubleData,
              Platform.DOUBLE_ARRAY_OFFSET, capacity * 8);
      Platform.copyMemory(null, nativeAddress + capacity * 8,
              nulls, Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof BinaryType || type instanceof StringType) {
      // lengthData::offsetData::nulls::child.data
      Platform.copyMemory(null, nativeAddress, arrayLengths,
              Platform.INT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 4, arrayOffsets,
              Platform.INT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 8,
              nulls, Platform.BYTE_ARRAY_OFFSET, capacity);
      // Need to determine the total length of data bytes.
      int lastIndex = capacity - 1;
      while (lastIndex >= 0 && isNullAt(lastIndex)) {
        lastIndex--;
      }
      if (lastIndex >= 0) {
        byte[] data = new byte[arrayOffsets[lastIndex] + arrayLengths[lastIndex]];
        Platform.copyMemory(null, nativeAddress + capacity * 9,
                data, Platform.BYTE_ARRAY_OFFSET,data.length);
        ((OnHeapColumnVector)this.childColumns[0]).byteData = data;
      }
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
  }
}
