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

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

/**
 * Add skip values ability to VectorizedColumnReader, skip method refer to
 * read method of VectorizedColumnReader.
 */
public class SkippableVectorizedColumnReader extends VectorizedColumnReader {

  public SkippableVectorizedColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
      throws IOException {
    super(descriptor, pageReader);
  }

  /**
   * Skip `total` values from this columnReader, ColumnVector used to
   * provide dataType and whether it is stored as array.
   */
  public void skipBatch(int total, ColumnVector vector) throws IOException {
    this.skipBatch(total, vector.dataType(), vector.isArray());
  }

  /**
   * Skip `total` values from this columnReader by dataType, and for Binary Type we need know
   * is it stored as array, when isArray is true, it's Binary String , else it's TimestampType.
   * This method refer to readBatch method in VectorizedColumnReader.
   */
  public void skipBatch(int total, DataType dataType, boolean isArray) throws IOException {
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      int num = Math.min(total, leftInPage);
      // isCurrentPageDictionaryEncoded re-assignment by readPage method.
      if (isCurrentPageDictionaryEncoded) {
        // If isCurrentPageDictionaryEncoded is true, dataType must be INT32, call skipIntegers.
        ((SkippableVectorizedRleValuesReader)defColumn)
          .skipIntegers(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
      } else {
        // isCurrentPageDictionaryEncoded is false, call skip by descriptor.getType(), this type
        // store in ColumnDescriptor of Parquet file.
        switch (descriptor.getType()) {
          case BOOLEAN:
            skipBooleanBatch(num, dataType);
            break;
          case INT32:
            skipIntBatch(num, dataType);
            break;
          case INT64:
            skipLongBatch(num, dataType);
            break;
          case INT96:
            skipBinaryBatch(num, dataType, isArray);
            break;
          case FLOAT:
            skipFloatBatch(num, dataType);
            break;
          case DOUBLE:
            skipDoubleBatch(num, dataType);
            break;
          case BINARY:
            skipBinaryBatch(num, dataType, isArray);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            skipFixedLenByteArrayBatch(num, dataType, descriptor.getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + descriptor.getType());
        }
      }

      valuesRead += num;
      total -= num;
    }
  }

  /**
   * For all the skip*Batch functions, skip `num` values from this columnReader. It
   * is guaranteed that num is smaller than the number of values left in the current page.
   */

  /**
   * BooleanType store as boolean, use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipBooleanBatch(int num, DataType dataType) {
    assert(dataType == DataTypes.BooleanType);
    ((SkippableVectorizedRleValuesReader)defColumn)
      .skipBooleans(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
  }

  /**
   * IntegerType | DateType | DecimalType(precision <= Decimal.MAX_INT_DIGITS) | ByteType
   * ShortType can store as int32, use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipIntBatch(int num, DataType dataType) {
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
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * LongType | DecimalType(precision <= Decimal.MAX_LONG_DIGITS) can store as int64,
   * use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipLongBatch(int num, DataType dataType) {
    if (dataType == DataTypes.LongType ||
                DecimalType.is64BitDecimalType(dataType)) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipLongs(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else {
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * FloatType store as float, use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipFloatBatch(int num, DataType dataType) {
    if (dataType == DataTypes.FloatType) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipFloats(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else {
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * DoubleType store as double, use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipDoubleBatch(int num, DataType dataType) {
    if (dataType == DataTypes.DoubleType) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipDoubles(num, maxDefLevel, (SkippableVectorizedValuesReader) dataColumn);
    } else {
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * ByteArray | TimestampType store as binary, use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipBinaryBatch(int num, DataType dataType, boolean isArray) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (isArray) {
      ((SkippableVectorizedRleValuesReader)defColumn)
        .skipBinarys(num, maxDefLevel, (SkippableVectorizedValuesReader) data);
    } else if (dataType == DataTypes.TimestampType) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          ((SkippableVectorizedValuesReader) data).skipBinaryByLen(12);
        }
      }
    } else {
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * Fix length decimal can store as FIXED_LEN_BYTE_ARRAY, use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipFixedLenByteArrayBatch(int num, DataType dataType, int arrayLen) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;

    if (DecimalType.is32BitDecimalType(dataType) || DecimalType.is64BitDecimalType(dataType)
      || DecimalType.isByteArrayDecimalType(dataType)) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          ((SkippableVectorizedValuesReader)data).skipBinaryByLen(arrayLen);
        }
      }
    } else {
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * Unified method to throw UnsupportedOperationException.
   */
  private void doThrowUnsupportedOperation(DataType dataType) {
    throw new UnsupportedOperationException("Unimplemented type: " + dataType);
  }

  /**
   * This method refer to initDataReader in VectorizedColumnReader,
   * just modified the assignment of dataColumn.
   */
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
      // VectorizedRleValuesReader -> SkippableVectorizedRleValuesReader
      this.dataColumn = new SkippableVectorizedRleValuesReader();
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      // VectorizedPlainValuesReader -> SkippableVectorizedPlainValuesReader
      this.dataColumn = new SkippableVectorizedPlainValuesReader();
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  /**
   * This method refer to readPageV1 in VectorizedColumnReader,
   * modified the assignment of defColumn and remove assignment to
   * repetitionLevelColumn & definitionLevelColumn because they are useless.
   */
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

  /**
   * This method refer to readPageV2 in VectorizedColumnReader,
   * modified the assignment of defColumn and remove assignment to
   * repetitionLevelColumn & definitionLevelColumn because they are useless.
   */
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
