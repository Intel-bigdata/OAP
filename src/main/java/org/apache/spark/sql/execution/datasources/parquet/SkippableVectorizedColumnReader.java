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

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

import java.io.IOException;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

public class SkippableVectorizedColumnReader extends VectorizedColumnReader {

  public SkippableVectorizedColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
      throws IOException {
    super(descriptor, pageReader);
  }

  /**
   * Reads `total` values from this columnReader into column.
   */
  public void skipBatch(int total, ColumnVector column) throws IOException {
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      int num = Math.min(total, leftInPage);
      if (isCurrentPageDictionaryEncoded) {
        // Read and decode dictionary ids.
        ((SkippableVectorizedRleValuesReader)defColumn)
          .skipIntegers(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
      } else {
        switch (descriptor.getType()) {
          case BOOLEAN:
            skipBooleanBatch(num, column);
            break;
          case INT32:
            skipIntBatch(num, column);
            break;
          case INT64:
            skipLongBatch(num, column);
            break;
          case INT96:
            skipBinaryBatch(num, column);
            break;
          case FLOAT:
            skipFloatBatch(num, column);
            break;
          case DOUBLE:
            skipDoubleBatch(num, column);
            break;
          case BINARY:
            skipBinaryBatch(num, column);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            skipFixedLenByteArrayBatch(num, column, descriptor.getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + descriptor.getType());
        }
      }

      valuesRead += num;
      total -= num;
    }
  }

  private void skipBooleanBatch(int num, ColumnVector column) {
    assert(column.dataType() == DataTypes.BooleanType);
    ((SkippableVectorizedRleValuesReader)defColumn)
      .skipBooleans(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
  }

  private void skipIntBatch(int num, ColumnVector column) {
    DataType dataType = column.dataType();
    if (dataType == DataTypes.IntegerType || dataType == DataTypes.DateType ||
                DecimalType.is32BitDecimalType(dataType)) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipIntegers(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else if (dataType == DataTypes.ByteType) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipBytes(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else if (dataType == DataTypes.ShortType) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipShorts(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + dataType);
    }
  }

  private void skipLongBatch(int num, ColumnVector column) {
    DataType dataType = column.dataType();
    if (dataType == DataTypes.LongType ||
                DecimalType.is64BitDecimalType(dataType)) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipLongs(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + dataType);
    }
  }

  private void skipFloatBatch(int num, ColumnVector column) {
    if (column.dataType() == DataTypes.FloatType) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipFloats(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
    }
  }

  private void skipDoubleBatch(int num, ColumnVector column) {
    if (column.dataType() == DataTypes.DoubleType) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipDoubles(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }


  private void skipBinaryBatch(int num, ColumnVector column) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (column.isArray()) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipBinarys(num, maxDefLevel, (SkippableVectorizedValuesReader) data);
    } else if (column.dataType() == DataTypes.TimestampType) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          ((SkippableVectorizedValuesReader) data).skipBinaryByLen(12);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void skipFixedLenByteArrayBatch(int num, ColumnVector column, int arrayLen) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    DataType dataType = column.dataType();

    if (DecimalType.is32BitDecimalType(dataType) || DecimalType.is64BitDecimalType(dataType)
      || DecimalType.isByteArrayDecimalType(dataType)) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          ((SkippableVectorizedValuesReader)data).skipBinaryByLen(arrayLen);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + dataType);
    }
  }

  @Override
  protected void initDataReader(Encoding dataEncoding, byte[] bytes, int offset)
    throws IOException {
    this.endOfPageValueCount = valuesRead + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      this.dataColumn = null;
      if (dictionary == null) {
        throw new IOException(
            "could not read page in col " + descriptor +
                " as the dictionary was missing for encoding " + dataEncoding);
      }
      @SuppressWarnings("deprecation")
      Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
      if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new SkippableVectorizedRleValuesReader();
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new SkippableVectorizedPlainValuesReader();
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  @Override
  protected void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader;

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new SkippableVectorizedRleValuesReader(bitWidth);
    dlReader = this.defColumn;
    try {
      byte[] bytes = page.getBytes().toByteArray();
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      initDataReader(page.getValueEncoding(), bytes, next);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  @Override
  protected void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new SkippableVectorizedRleValuesReader(bitWidth);
    this.defColumn.initFromBuffer(
            this.pageValueCount, page.getDefinitionLevels().toByteArray());
    try {
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
