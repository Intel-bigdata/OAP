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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.io.api.Binary

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFiberBuilder
import org.apache.spark.sql.execution.datasources.oap.io.meta.{ColumnMetaV1, ColumnStatistics, OapDataFileMetaV1, RowGroupMetaV1}
import org.apache.spark.sql.types._

// TODO: [linhong] Let's remove the `isCompressed` argument
// Note: For Oap dataFile writer, we only main a single latest version
private[oap] class OapDataWriter(
    isCompressed: Boolean,
    out: FSDataOutputStream,
    schema: StructType,
    conf: Configuration) extends Logging {

  private val ROW_GROUP_SIZE =
    conf.get(OapFileFormat.ROW_GROUP_SIZE, OapFileFormat.DEFAULT_ROW_GROUP_SIZE).toInt
  logDebug(s"${OapFileFormat.ROW_GROUP_SIZE} setting to $ROW_GROUP_SIZE")

  private val COMPRESSION_CODEC = CompressionCodec.valueOf(
    conf.get(OapFileFormat.COMPRESSION, OapFileFormat.DEFAULT_COMPRESSION).toUpperCase())
  logDebug(s"${OapFileFormat.COMPRESSION} setting to ${COMPRESSION_CODEC.name()}")

  private var rowCount: Int = 0
  private var rowGroupCount: Int = 0

  private val rowGroup: Array[DataFiberBuilder] =
    DataFiberBuilder.initializeFromSchema(schema, ROW_GROUP_SIZE)

  private val fileStatiscs = ColumnStatistics.getStatsFromSchema(schema)
  private var rowGroupstatistics = ColumnStatistics.getStatsFromSchema(schema)

  private def updateStats(
      stats: ColumnStatistics.ParquetStatistics,
      row: InternalRow,
      index: Int,
      dataType: DataType): Unit = {
    dataType match {
      case BooleanType => stats.updateStats(row.getBoolean(index))
      case IntegerType => stats.updateStats(row.getInt(index))
      case ByteType => stats.updateStats(row.getByte(index))
      case DateType => stats.updateStats(row.getInt(index))
      case ShortType => stats.updateStats(row.getShort(index))
      case StringType => stats.updateStats(
        Binary.fromConstantByteArray(row.getString(index).getBytes))
      case BinaryType => stats.updateStats(
        Binary.fromConstantByteArray(row.getBinary(index)))
      case FloatType => stats.updateStats(row.getFloat(index))
      case DoubleType => stats.updateStats(row.getDouble(index))
      case LongType => stats.updateStats(row.getLong(index))
      case _ => sys.error(s"Not support data type: $dataType")
    }
  }

  private val fiberMeta = new OapDataFileMetaV1(
    rowCountInEachGroup = ROW_GROUP_SIZE,
    fieldCount = schema.length,
    codec = COMPRESSION_CODEC)

  private val codecFactory = new CodecFactory(conf)

  def write(row: InternalRow) {
    rowGroup.zipWithIndex.foreach { case (dataFiberBuilder, i) =>
      dataFiberBuilder.append(row)
      if (!row.isNullAt(i)) {
        updateStats(fileStatiscs(i), row, i, schema(i).dataType)
        updateStats(rowGroupstatistics(i), row, i, schema(i).dataType)
      }
    }
    rowCount += 1
    if (rowCount % ROW_GROUP_SIZE == 0) {
      writeRowGroup()
    }
  }

  private def writeRowGroup(): Unit = {
    rowGroupCount += 1
    val compressor: BytesCompressor = codecFactory.getCompressor(COMPRESSION_CODEC)
    val fiberLens = new Array[Int](rowGroup.length)
    val fiberUncompressedLens = new Array[Int](rowGroup.length)
    var idx: Int = 0
    var totalDataSize = 0L
    val rowGroupMeta = new RowGroupMetaV1()

    rowGroupMeta.withNewStart(out.getPos)
        .withNewFiberLens(fiberLens)
        .withNewUncompressedFiberLens(fiberUncompressedLens)
        .withNewStatistics(rowGroupstatistics.map(ColumnStatistics(_)).toArray)

    while (idx < rowGroup.length) {
      val fiberByteData = rowGroup(idx).build()
      val newUncompressedFiberData = fiberByteData.fiberData
      val newFiberData = compressor.compress(newUncompressedFiberData)
      totalDataSize += newFiberData.length
      fiberLens(idx) = newFiberData.length
      fiberUncompressedLens(idx) = newUncompressedFiberData.length
      out.write(newFiberData)
      rowGroup(idx).clear()
      idx += 1
    }
    rowGroupstatistics = ColumnStatistics.getStatsFromSchema(schema)
    fiberMeta.appendRowGroupMeta(rowGroupMeta.withNewEnd(out.getPos))
  }

  def close() {
    val remainingRowCount = rowCount % ROW_GROUP_SIZE
    if (remainingRowCount != 0) {
      // should be end of the insertion, put the row groups into the last row group
      writeRowGroup()
    }

    rowGroup.zipWithIndex.foreach { case (dataFiberBuilder, i) =>
      val dictByteData = dataFiberBuilder.buildDictionary
      val encoding = dataFiberBuilder.getEncoding
      val dictionaryDataLength = dictByteData.length
      val dictionaryIdSize = dataFiberBuilder.getDictionarySize
      if (dictionaryDataLength > 0) {
        out.write(dictByteData)
      }
      fiberMeta.appendColumnMeta(
        new ColumnMetaV1(
          encoding, dictionaryDataLength, dictionaryIdSize, ColumnStatistics(fileStatiscs(i))))
    }

    // and update the group count and row count in the last group
    fiberMeta
        .withGroupCount(rowGroupCount)
        .withRowCountInLastGroup(
          if (remainingRowCount != 0 || rowCount == 0) remainingRowCount else ROW_GROUP_SIZE)

    fiberMeta.write(out)
    codecFactory.release()
    out.close()
  }
}

