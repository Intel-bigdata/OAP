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
import java.util.Arrays;
import java.util.TimeZone;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

/**
 * Add skip values ability to VectorizedColumnReader, skip method refer to
 * read method of VectorizedColumnReader.
 */
public class SkippableVectorizedColumnReader {

  /**
   * Total number of values read.
   */
  protected long valuesRead;

  /**
   * value that indicates the end of the current page. That is,
   * if valuesRead == endOfPageValueCount, we are at the end of the page.
   */
  protected long endOfPageValueCount;

  /**
   * The dictionary, if this column has dictionary encoding.
   */
  protected final Dictionary dictionary;

  /**
   * If true, the current page is dictionary encoded.
   */
  protected boolean isCurrentPageDictionaryEncoded;

  /**
   * Maximum definition level for this column.
   */
  protected final int maxDefLevel;

  private SpecificParquetRecordReaderBase.IntIterator definitionLevelColumn;

  protected ValuesReader dataColumn;

  /**
   * Reference dataColumn, but Type is SkippableVectorizedValuesReader, reduce manual cast
   * times.
   */
  private SkippableVectorizedValuesReader dataColumnRef;

  /**
   * Reference defColumn, but Type is SkippableVectorizedRleValuesReader, reduce manual cast
   * times.
   */
  private SkippableVectorizedRleValuesReader defColumnRef;

  /**
   * Total number of values in this column (in this row group).
   */
  private final long totalValueCount;

  /**
   * Total values in the current page.
   */
  protected int pageValueCount;

  private final PageReader pageReader;
  protected final ColumnDescriptor descriptor;
  protected final OriginalType originalType;
  // The timezone conversion to apply to int96 timestamps. Null if no conversion.
  private final TimeZone convertTz;
  private static final TimeZone UTC = DateTimeUtils.TimeZoneUTC();

   public SkippableVectorizedColumnReader(
           ColumnDescriptor descriptor,
           OriginalType originalType,
           PageReader pageReader,
           TimeZone convertTz) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.convertTz = convertTz;
    this.originalType = originalType;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        this.isCurrentPageDictionaryEncoded = true;
      } catch (IOException e) {
        throw new IOException("could not decode the dictionary for " + descriptor, e);
      }
    } else {
      this.dictionary = null;
      this.isCurrentPageDictionaryEncoded = false;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new IOException("totalValueCount == 0");
    }
  }

  /**
   * Advances to the next value. Returns true if the value is non-null.
   */
  private boolean next() throws IOException {
    if (valuesRead >= endOfPageValueCount) {
      if (valuesRead >= totalValueCount) {
        // How do we get here? Throw end of stream exception?
        return false;
      }
      readPage();
    }
    ++valuesRead;
    // TODO: Don't read for flat schemas
    //repetitionLevel = repetitionLevelColumn.nextInt();
    return definitionLevelColumn.nextInt() == maxDefLevel;
  }

  /**
   * Reads `total` values from this columnReader into column.
   */
  public void readBatch(int total, WritableColumnVector column) throws IOException {
    int rowId = 0;
    WritableColumnVector dictionaryIds = null;
    if (dictionary != null) {
      // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be used to
      // decode all previous dictionary encoded pages if we ever encounter a non-dictionary encoded
      // page.
      dictionaryIds = column.reserveDictionaryIds(total);
    }
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      int num = Math.min(total, leftInPage);
      PrimitiveType.PrimitiveTypeName typeName =
              descriptor.getPrimitiveType().getPrimitiveTypeName();
      if (isCurrentPageDictionaryEncoded) {
        // Read and decode dictionary ids.
        defColumnRef.readIntegers(num, dictionaryIds,
                column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);

        // TIMESTAMP_MILLIS encoded as INT64 can't be lazily decoded as we need to post process
        // the values to add microseconds precision.
        if (column.hasDictionary() || (rowId == 0 &&
                (typeName == PrimitiveType.PrimitiveTypeName.INT32 ||
                        (typeName == PrimitiveType.PrimitiveTypeName.INT64 &&
                                originalType != OriginalType.TIMESTAMP_MILLIS) ||
                        typeName == PrimitiveType.PrimitiveTypeName.FLOAT ||
                        typeName == PrimitiveType.PrimitiveTypeName.DOUBLE ||
                        typeName == PrimitiveType.PrimitiveTypeName.BINARY))) {
          // Column vector supports lazy decoding of dictionary values so just set the dictionary.
          // We can't do this if rowId != 0 AND the column doesn't have a dictionary (i.e. some
          // non-dictionary encoded values have already been added).
          column.setDictionary(new ParquetDictionaryWrapper(dictionary));
        } else {
          decodeDictionaryIds(rowId, num, column, dictionaryIds);
        }
      } else {
        if (column.hasDictionary() && rowId != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not so we will decode the dictionary.
          decodeDictionaryIds(0, rowId, column, column.getDictionaryIds());
        }
        column.setDictionary(null);
        switch (typeName) {
          case BOOLEAN:
            readBooleanBatch(rowId, num, column);
            break;
          case INT32:
            readIntBatch(rowId, num, column);
            break;
          case INT64:
            readLongBatch(rowId, num, column);
            break;
          case INT96:
            readBinaryBatch(rowId, num, column);
            break;
          case FLOAT:
            readFloatBatch(rowId, num, column);
            break;
          case DOUBLE:
            readDoubleBatch(rowId, num, column);
            break;
          case BINARY:
            readBinaryBatch(rowId, num, column);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            readFixedLenByteArrayBatch(
                    rowId, num, column, descriptor.getPrimitiveType().getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + typeName);
        }
      }

      valuesRead += num;
      rowId += num;
      total -= num;
    }
  }

  private boolean shouldConvertTimestamps() {
    return convertTz != null && !convertTz.equals(UTC);
  }

  /**
   * Helper function to construct exception for parquet schema mismatch.
   */
  private SchemaColumnConvertNotSupportedException constructConvertNotSupportedException(
          ColumnDescriptor descriptor,
          WritableColumnVector column) {
    return new SchemaColumnConvertNotSupportedException(
            Arrays.toString(descriptor.getPath()),
            descriptor.getPrimitiveType().getPrimitiveTypeName().toString(),
            column.dataType().catalogString());
  }

  /**
   * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
   */
  private void decodeDictionaryIds(
          int rowId,
          int num,
          WritableColumnVector column,
          WritableColumnVector dictionaryIds) {
    switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
      case INT32:
        if (column.dataType() == DataTypes.IntegerType ||
                DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putInt(i, dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ByteType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putByte(i, (byte) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ShortType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putShort(i, (short) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      case INT64:
        if (column.dataType() == DataTypes.LongType ||
                DecimalType.is64BitDecimalType(column.dataType()) ||
                originalType == OriginalType.TIMESTAMP_MICROS) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i, dictionary.decodeToLong(dictionaryIds.getDictId(i)));
            }
          }
        } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i,
                DateTimeUtils.fromMillis(dictionary.decodeToLong(dictionaryIds.getDictId(i))));
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      case FLOAT:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putFloat(i, dictionary.decodeToFloat(dictionaryIds.getDictId(i)));
          }
        }
        break;

      case DOUBLE:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putDouble(i, dictionary.decodeToDouble(dictionaryIds.getDictId(i)));
          }
        }
        break;
      case INT96:
        if (column.dataType() == DataTypes.TimestampType) {
          if (!shouldConvertTimestamps()) {
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                column.putLong(i, ParquetRowConverter.binaryToSQLTimestamp(v));
              }
            }
          } else {
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                long rawTime = ParquetRowConverter.binaryToSQLTimestamp(v);
                long adjTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
                column.putLong(i, adjTime);
              }
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;
      case BINARY:
        // TODO: this is incredibly inefficient as it blows up the dictionary right here. We
        // need to do this better. We should probably add the dictionary data to the ColumnVector
        // and reuse it across batches. This should mean adding a ByteArray would just update
        // the length and offset.
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
            column.putByteArray(i, v.getBytes());
          }
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        // DecimalType written in the legacy mode
        if (DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putInt(i, (int) ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.is64BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putLong(i, ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putByteArray(i, v.getBytes());
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      default:
        throw new UnsupportedOperationException(
                "Unsupported type: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  /**
   * For all the read*Batch functions, reads `num` values from this columnReader into column. It
   * is guaranteed that num is smaller than the number of values left in the current page.
   */

  private void readBooleanBatch(int rowId, int num, WritableColumnVector column)
          throws IOException {
    if (column.dataType() != DataTypes.BooleanType) {
      throw constructConvertNotSupportedException(descriptor, column);
    }
    defColumnRef.readBooleans(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
  }

  private void readIntBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.IntegerType || column.dataType() == DataTypes.DateType ||
            DecimalType.is32BitDecimalType(column.dataType())) {
      defColumnRef.readIntegers(
              num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ByteType) {
      defColumnRef.readBytes(
              num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ShortType) {
      defColumnRef.readShorts(
              num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readLongBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    if (column.dataType() == DataTypes.LongType ||
            DecimalType.is64BitDecimalType(column.dataType()) ||
            originalType == OriginalType.TIMESTAMP_MICROS) {
      defColumnRef.readLongs(
              num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
      for (int i = 0; i < num; i++) {
        if (defColumnRef.readInteger() == maxDefLevel) {
          column.putLong(rowId + i, DateTimeUtils.fromMillis(dataColumn.readLong()));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readFloatBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: support implicit cast to double?
    if (column.dataType() == DataTypes.FloatType) {
      defColumnRef.readFloats(
              num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readDoubleBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.DoubleType) {
      defColumnRef.readDoubles(
              num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readBinaryBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (column.dataType() == DataTypes.StringType || column.dataType() == DataTypes.BinaryType
            || DecimalType.isByteArrayDecimalType(column.dataType())) {
      defColumnRef.readBinarys(num, column, rowId, maxDefLevel, data);
    } else if (column.dataType() == DataTypes.TimestampType) {
      if (!shouldConvertTimestamps()) {
        for (int i = 0; i < num; i++) {
          if (dataColumnRef.readInteger() == maxDefLevel) {
            // Read 12 bytes for INT96
            long rawTime = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
            column.putLong(rowId + i, rawTime);
          } else {
            column.putNull(rowId + i);
          }
        }
      } else {
        for (int i = 0; i < num; i++) {
          if (defColumnRef.readInteger() == maxDefLevel) {
            // Read 12 bytes for INT96
            long rawTime = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
            long adjTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
            column.putLong(rowId + i, adjTime);
          } else {
            column.putNull(rowId + i);
          }
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readFixedLenByteArrayBatch(
          int rowId,
          int num,
          WritableColumnVector column,
          int arrayLen) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (DecimalType.is32BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumnRef.readInteger() == maxDefLevel) {
          column.putInt(rowId + i,
                  (int) ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is64BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumnRef.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
                  ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumnRef.readInteger() == maxDefLevel) {
          column.putByteArray(rowId + i, data.readBinary(arrayLen).getBytes());
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  protected void readPage() {
    DataPage page = pageReader.readPage();
    // TODO: Why is this a visitor?
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        try {
          readPageV1(dataPageV1);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        try {
          readPageV2(dataPageV2);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * Skip `total` values from this columnReader, ColumnVector used to
   * provide dataType and whether it is stored as array.
   */
  public void skipBatch(int total, WritableColumnVector vector) throws IOException {
    this.skipBatch(total, vector.dataType());
  }

  /**
   * Skip `total` values from this columnReader by dataType, and for Binary Type we need know
   * is it stored as array, when isArray is true, it's Binary String , else it's TimestampType.
   * This method refer to readBatch method in VectorizedColumnReader.
   */
  public void skipBatch(int total, DataType dataType) throws IOException {
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
        ((SkippableVectorizedRleValuesReader)defColumnRef)
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
            skipBinaryBatch(num, dataType);
            break;
          case FLOAT:
            skipFloatBatch(num, dataType);
            break;
          case DOUBLE:
            skipDoubleBatch(num, dataType);
            break;
          case BINARY:
            skipBinaryBatch(num, dataType);
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
    if (dataType == DataTypes.BooleanType) {
      defColumnRef.skipBooleans(num, maxDefLevel, dataColumnRef);
    } else {
      doThrowUnsupportedOperation(dataType);
    }
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
      defColumnRef.skipIntegers(num, maxDefLevel, dataColumnRef);
    } else if (dataType == DataTypes.ByteType) {
      defColumnRef.skipBytes(num, maxDefLevel, dataColumnRef);
    } else if (dataType == DataTypes.ShortType) {
      defColumnRef.skipShorts(num, maxDefLevel, dataColumnRef);
    } else {
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * LongType | DecimalType(precision <= Decimal.MAX_LONG_DIGITS) |
   * OriginalType.TIMESTAMP_MICROS | OriginalType.TIMESTAMP_MILLIS can store as int64,
   * use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipLongBatch(int num, DataType dataType) {
    if (dataType == DataTypes.LongType ||
      DecimalType.is64BitDecimalType(dataType) ||
      originalType == OriginalType.TIMESTAMP_MICROS ||
      originalType == OriginalType.TIMESTAMP_MILLIS) {
      defColumnRef.skipLongs(num, maxDefLevel, dataColumnRef);
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
      defColumnRef.skipFloats(num, maxDefLevel, dataColumnRef);
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
      defColumnRef.skipDoubles(num, maxDefLevel, dataColumnRef);
    } else {
      doThrowUnsupportedOperation(dataType);
    }
  }

  /**
   * StringType | ByteArray | TimestampType | DecimalType.isByteArrayDecimalType
   * store as binary, use this method to skip records.
   * @param num record count
   * @param dataType dataType
   */
  private void skipBinaryBatch(int num, DataType dataType) {
    if (dataType == DataTypes.StringType ||
       dataType == DataTypes.BinaryType ||
       DecimalType.isByteArrayDecimalType(dataType)) {
      defColumnRef.skipBinarys(num, maxDefLevel, dataColumnRef);
    } else if (dataType == DataTypes.TimestampType) {
      for (int i = 0; i < num; i++) {
        if (defColumnRef.readInteger() == maxDefLevel) {
          dataColumnRef.skipBinaryByLen(12);
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
    if (DecimalType.is32BitDecimalType(dataType) || DecimalType.is64BitDecimalType(dataType) ||
      DecimalType.isByteArrayDecimalType(dataType)) {
      for (int i = 0; i < num; i++) {
        if (defColumnRef.readInteger() == maxDefLevel) {
          dataColumnRef.skipBinaryByLen(arrayLen);
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
  protected void initDataReader(Encoding dataEncoding, ByteBufferInputStream in)
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
      dataColumn.initFromPage(pageValueCount, in);
      // dataColumnRef reference dataColumn and type is SkippableVectorizedValuesReader
      this.dataColumnRef = (SkippableVectorizedValuesReader)this.dataColumn;
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  /**
   * This method refer to readPageV1 in VectorizedColumnReader,
   * modified the assignment of defColumn and remove assignment to
   * repetitionLevelColumn & definitionLevelColumn because they are useless.
   */
  protected void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader;

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumnRef = new SkippableVectorizedRleValuesReader(bitWidth);
    // defColumnRef reference defColumn and type is SkippableVectorizedRleValuesReader
    this.defColumnRef = (SkippableVectorizedRleValuesReader)this.defColumnRef;
    dlReader = this.defColumnRef;
    try {
      BytesInput bytes = page.getBytes();
      ByteBufferInputStream in = bytes.toInputStream();
      rlReader.initFromPage(pageValueCount, in);
      dlReader.initFromPage(pageValueCount, in);
      initDataReader(page.getValueEncoding(), in);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  /**
   * This method refer to readPageV2 in VectorizedColumnReader,
   * modified the assignment of defColumn and remove assignment to
   * repetitionLevelColumn & definitionLevelColumn because they are useless.
   */
  protected void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    // do not read the length from the stream. v2 pages handle dividing the page bytes.
    this.defColumnRef = new SkippableVectorizedRleValuesReader(bitWidth, false);
    this.defColumnRef.initFromPage(
            this.pageValueCount, page.getDefinitionLevels().toInputStream());
    // defColumnRef reference defColumn and type is SkippableVectorizedRleValuesReader
    this.defColumnRef = (SkippableVectorizedRleValuesReader) this.defColumnRef;
    try {
      initDataReader(page.getDataEncoding(), page.getData().toInputStream());
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
