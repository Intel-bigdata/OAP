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

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.oap.io.FiberUsable;
import org.apache.spark.sql.oap.OapRuntime$;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public class OapOnHeapColumnVectorFiber implements FiberUsable, Closeable {

  private final OnHeapColumnVector vector;
  private final int capacity;
  private final DataType type;

  public OapOnHeapColumnVectorFiber(int capacity, DataType type) {
    this.vector = new OnHeapColumnVector(capacity, type);
    this.capacity = capacity;
    this.type = type;
  }

  public OapOnHeapColumnVectorFiber(OnHeapColumnVector vector) {
    this.vector = vector;
    this.capacity = (int)OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "capacity");
    this.type = (DataType)OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "type");
  }


  @Override
  public void dumpBytesToCache(long nativeAddress) {
    if (type instanceof ByteType) {
      // data: 1 byte, nulls: 1 byte
      if (withOutDictionary()) {
        Platform.copyMemory(byteData(), Platform.BYTE_ARRAY_OFFSET, null,
                nativeAddress, capacity);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putByte(null, nativeAddress + i, getByte(i));
          }
        }
      }
      Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity, capacity);
    } else if (type instanceof BooleanType) {
      // data: 1 byte, nulls: 1 byte
      if (withOutDictionary()) {
        Platform.copyMemory(byteData(), Platform.BYTE_ARRAY_OFFSET, null,
                nativeAddress, capacity);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putByte(null, nativeAddress + i,
                    (byte) ((getBoolean(i)) ? 1 : 0));
          }
        }
      }
      Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity, capacity);
    } else if (type instanceof ShortType) {
      // data: 2 bytes, nulls: 1 byte
      if (withOutDictionary()) {
        Platform.copyMemory(shortData(), Platform.SHORT_ARRAY_OFFSET, null,
                nativeAddress, capacity * 2);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putShort(null, nativeAddress + i * 2, getShort(i));
          }
        }
      }
      Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 2, capacity);
    } else if (type instanceof IntegerType || type instanceof DateType) {
      // data: 4 bytes, nulls: 1 byte
      if (withOutDictionary()) {
        Platform.copyMemory(intData(), Platform.INT_ARRAY_OFFSET, null,
                nativeAddress, capacity * 4);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putInt(null, nativeAddress + i * 4, getInt(i));
          }
        }
      }
      Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 4, capacity);
    } else if (type instanceof FloatType) {
      // data: 4 bytes, nulls: 1 byte
      if (withOutDictionary()) {
        Platform.copyMemory(floatData(), Platform.FLOAT_ARRAY_OFFSET, null,
                nativeAddress, capacity * 4);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putFloat(null, nativeAddress + i * 4, getFloat(i));
          }
        }
      }
      Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 4, capacity);
    } else if (type instanceof LongType) {
      // data: 8 bytes, nulls: 1 byte
      if (withOutDictionary()) {
        Platform.copyMemory(longData(), Platform.LONG_ARRAY_OFFSET, null,
                nativeAddress, capacity * 8);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putLong(null, nativeAddress + i * 8, getLong(i));
          }
        }
      }
      Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, null,
              nativeAddress + capacity * 8, capacity);
    } else if (type instanceof DoubleType) {
      // data: 8 bytes, nulls: 1 byte
      if (withOutDictionary()) {
        Platform.copyMemory(doubleData(), Platform.DOUBLE_ARRAY_OFFSET, null,
                nativeAddress, capacity * 8);
      } else {
        for (int i = 0; i < capacity; i++) {
          if (!isNullAt(i)) {
            Platform.putDouble(null, nativeAddress + i * 8, getDouble(i));
          }
        }
      }
      Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, null,
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
      if (withOutDictionary()) {
        dataBytes = new byte[capacity * (4 + 4 + 1) + getChildColumn0().elementsAppended];
        Platform.copyMemory(arrayLengths(), Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 4);
        Platform.copyMemory(arrayOffsets(), Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 4, capacity * 4);
        Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = byteData(getChildColumn0());
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                getChildColumn0().elementsAppended);
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
        dataBytes = new byte[capacity * (4 + 4 + 1) + getChildColumn0().elementsAppended];
        Platform.copyMemory(tempBytes, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 8);
        Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET , dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = byteData(getChildColumn0());
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                getChildColumn0().elementsAppended);
      }
    } else if (type instanceof StringType) {
      // lengthData: 4 bytes, offsetData: 4 bytes, nulls: 1 byte,
      // child.data: childColumns[0].elementsAppended bytes.
      if (withOutDictionary()) {
        dataBytes = new byte[capacity * (4 + 4 + 1) + getChildColumn0().elementsAppended];
        Platform.copyMemory(arrayLengths(), Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 4);
        Platform.copyMemory(arrayOffsets(), Platform.INT_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 4, capacity * 4);
        Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = byteData(getChildColumn0());
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                getChildColumn0().elementsAppended);
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
        dataBytes = new byte[capacity * (4 + 4 + 1) + getChildColumn0().elementsAppended];
        Platform.copyMemory(tempBytes, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET, capacity * 8);
        Platform.copyMemory(nulls(), Platform.BYTE_ARRAY_OFFSET , dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 8, capacity);
        byte[] data = byteData(getChildColumn0());
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, dataBytes,
                Platform.BYTE_ARRAY_OFFSET + capacity * 9,
                getChildColumn0().elementsAppended);
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
      Platform.copyMemory(null, nativeAddress, byteData(),
              Platform.BYTE_ARRAY_OFFSET, capacity);
      Platform.copyMemory(null, nativeAddress + capacity,
              nulls(), Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof ShortType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, shortData(),
              Platform.SHORT_ARRAY_OFFSET, capacity * 2);
      Platform.copyMemory(null, nativeAddress + capacity * 2,
              nulls(), Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof IntegerType || type instanceof DateType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, intData(),
              Platform.INT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 4,
              nulls(), Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof FloatType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, floatData(),
              Platform.FLOAT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 4,
              nulls(), Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof LongType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, longData(),
              Platform.LONG_ARRAY_OFFSET, capacity * 8);
      Platform.copyMemory(null, nativeAddress + capacity * 8,
              nulls(), Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof DoubleType) {
      // data::nulls
      Platform.copyMemory(null, nativeAddress, doubleData(),
              Platform.DOUBLE_ARRAY_OFFSET, capacity * 8);
      Platform.copyMemory(null, nativeAddress + capacity * 8,
              nulls(), Platform.BYTE_ARRAY_OFFSET, capacity);
    } else if (type instanceof BinaryType || type instanceof StringType) {
      // lengthData::offsetData::nulls::child.data
      Platform.copyMemory(null, nativeAddress, arrayLengths(),
              Platform.INT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 4, arrayOffsets(),
              Platform.INT_ARRAY_OFFSET, capacity * 4);
      Platform.copyMemory(null, nativeAddress + capacity * 8,
              nulls(), Platform.BYTE_ARRAY_OFFSET, capacity);
      // Need to determine the total length of data bytes.
      int lastIndex = capacity - 1;
      while (lastIndex >= 0 && isNullAt(lastIndex)) {
        lastIndex--;
      }
      if (lastIndex >= 0) {
        byte[] data = new byte[getArrayOffset(lastIndex) + getArrayLength(lastIndex)];
        Platform.copyMemory(null, nativeAddress + capacity * 9,
                data, Platform.BYTE_ARRAY_OFFSET,data.length);
        setByteData(getChildColumn0(), data);
      }
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
  }

  private byte getByte(int rowId) {
    return vector.getByte(rowId);
  }

  private boolean getBoolean(int rowId) {
    return vector.getBoolean(rowId);
  }

  private short getShort(int rowId) {
    return vector.getShort(rowId);
  }

  private int getInt(int rowId) {
    return vector.getInt(rowId);
  }

  private float getFloat(int rowId) {
    return vector.getFloat(rowId);
  }

  private long getLong(int rowId) {
    return vector.getLong(rowId);
  }

  private double getDouble(int rowId) {
    return vector.getDouble(rowId);
  }

  private byte[] getBinary(int rowId) {
    return vector.getBinary(rowId);
  }

  private boolean withOutDictionary() {
    return !vector.hasDictionary();
  }

  private boolean isNullAt(int rowId) {
    return vector.isNullAt(rowId);
  }

  private ColumnVector arrayData() {
    return vector.arrayData();
  }

  private UTF8String getUTF8String(int rowId) {
    return vector.getUTF8String(rowId);
  }

  private OnHeapColumnVector getChildColumn0() {
    return (OnHeapColumnVector)vector.getChildColumn(0);
  }

  private int getArrayOffset(int rowId) {
    return vector.getArrayOffset(rowId);
  }

  private int getArrayLength(int rowId) {
    return vector.getArrayLength(rowId);
  }

  private Object nulls() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "nulls");
  }

  private Object byteData() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "byteData");
  }

  private void setByteData(OnHeapColumnVector columnVector, byte[] data) {
    OnHeapCoumnVectorFiledAccessor.setFieldValue(columnVector, "byteData", data);
  }

  private byte[] byteData(OnHeapColumnVector columnVector) {
    return (byte[])OnHeapCoumnVectorFiledAccessor.getFieldValue(columnVector, "byteData");
  }

  private Object shortData() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "shortData");
  }

  private Object intData() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "intData");
  }

  private Object floatData() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "floatData");
  }

  private Object longData() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "longData");
  }

  private Object doubleData() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "doubleData");
  }

  private Object arrayLengths() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "arrayLengths");
  }

  private Object arrayOffsets() {
    return OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "arrayOffsets");
  }

  public OnHeapColumnVector getVector() {
    return vector;
  }

  @Override
  public void close() throws IOException {
    vector.close();
  }
}
