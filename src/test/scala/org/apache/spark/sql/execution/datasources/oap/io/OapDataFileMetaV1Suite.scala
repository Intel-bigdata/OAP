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

import java.io.{ByteArrayInputStream, DataInputStream, File}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.{CompressionCodec, Encoding}
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.oap.adapter.PropertiesAdapter
import org.apache.spark.util.Utils

class OapDataFileMetaCheck extends Properties("OapDataFileMeta") {
  private val tmpDir = Utils.createTempDir()
  private val conf = new Configuration()

  private lazy val genOapDataFileMeta: Gen[OapDataFileMetaV1] = {
    // TODO: [linhong] Need determine the range of each value.
    for { rowGroupCount <- Gen.choose[Int](1, 1000)
      defaultRowCount <- Gen.choose[Int](1, 1048576)
      fieldCount <- Gen.choose[Int](1, 300)
      lastRowCount <- Gen.choose[Int](1, defaultRowCount)
      fiberLens <- Gen.listOfN(fieldCount, Gen.choose[Int](0, 1048576))
      uncompressedFiberLens <- Gen.listOfN(fieldCount, Gen.choose[Int](0, 1048576))
      codec <- Gen.oneOf(CompressionCodec.GZIP,
        CompressionCodec.LZO,
        CompressionCodec.GZIP,
        CompressionCodec.UNCOMPRESSED)
      columnsMeta <- Gen.listOfN(fieldCount, arbitrary[ColumnMeta])
      rowGroupStatistics <- Gen.listOfN(fieldCount, arbitrary[ColumnStatistics])
    } yield generateOapDataFileMeta(rowGroupCount,
      defaultRowCount,
      fieldCount,
      lastRowCount,
      fiberLens.toArray,
      uncompressedFiberLens.toArray,
      columnsMeta,
      codec,
      rowGroupStatistics.toArray)
  }

  implicit lazy val arbOapDataFileMeta: Arbitrary[OapDataFileMetaV1] = {
    Arbitrary(genOapDataFileMeta)
  }

  private def generateOapDataFileMeta(
      rowGroupCount: Int,
      defaultRowCount: Int,
      fieldCount: Int,
      lastRowCount: Int,
      fiberLens: Array[Int],
      uncompressedFiberLens: Array[Int],
      columnsMeta: Seq[ColumnMeta],
      codec: CompressionCodec,
      rowGroupStatistics: Array[ColumnStatistics]): OapDataFileMetaV1 = {

    val rowGroupMetaArray = new Array[RowGroupMeta](rowGroupCount)
    rowGroupMetaArray.indices.foreach(
      rowGroupMetaArray(_) = new RowGroupMeta()
        .withNewStart(0)
        .withNewEnd(100)
        .withNewFiberLens(fiberLens)
        .withNewUncompressedFiberLens(uncompressedFiberLens)
        .withNewStatistics(rowGroupStatistics)
    )

    val oapDataFileMeta = new OapDataFileMetaV1(
      rowCountInEachGroup = defaultRowCount,
      fieldCount = fieldCount,
      codec = codec
    )

    oapDataFileMeta
      .withGroupCount(rowGroupCount)
      .withRowCountInLastGroup(lastRowCount)

    rowGroupMetaArray.foreach(oapDataFileMeta.appendRowGroupMeta)
    columnsMeta.foreach(oapDataFileMeta.appendColumnMeta)

    oapDataFileMeta
  }

  private lazy val genColumnMeta: Gen[ColumnMeta] = {
    for {
      encoding <- Gen.oneOf(Encoding.PLAIN, Encoding.RLE, Encoding.RLE_DICTIONARY,
        Encoding.DELTA_LENGTH_BYTE_ARRAY, Encoding.DELTA_BINARY_PACKED)
      dictionaryDataLength <- Gen.posNum[Int]
      dictionaryIdSize <- Gen.posNum[Int]
      statistics <- arbitrary[ColumnStatistics]
    } yield {
      new ColumnMeta(encoding, dictionaryDataLength, dictionaryIdSize, statistics)
    }
  }

  implicit lazy val arbColumnMeta: Arbitrary[ColumnMeta] = Arbitrary(genColumnMeta)

  private lazy val genColumnStatistics: Gen[ColumnStatistics] = {
    for { min <- arbitrary[Array[Byte]]
          max <- arbitrary[Array[Byte]]
    } yield {
        if (min.isEmpty || max.isEmpty) {
          new ColumnStatistics(null, null)
        } else {
          new ColumnStatistics(min, max)
        }
      }
  }

  implicit lazy val arbColumnStatistics: Arbitrary[ColumnStatistics] = {
    Arbitrary(genColumnStatistics)
  }

  private def isEqual(l: RowGroupMeta, r: RowGroupMeta): Boolean = {
    l.start == r.start && l.end == r.end &&
      (l.fiberLens sameElements r.fiberLens) &&
      (l.fiberUncompressedLens sameElements r.fiberUncompressedLens) &&
      l.statistics.zip(r.statistics).forall {
      case (left, right) => isEqual(left, right)
      }
  }

  private def isEqual(l: OapDataFileMetaV1, r: OapDataFileMetaV1): Boolean = {

    l.rowCountInEachGroup == r.rowCountInEachGroup &&
      l.rowCountInLastGroup == r.rowCountInLastGroup &&
      l.groupCount == r.groupCount &&
      l.fieldCount == r.fieldCount &&
      l.codec == r.codec &&
      l.rowGroupsMeta.length == r.rowGroupsMeta.length &&
      l.columnsMeta.length == r.columnsMeta.length &&
      l.columnsMeta.zip(r.columnsMeta).forall {
        case (left, right) => isEqual(left, right)
      } &&
      l.rowGroupsMeta.zip(r.rowGroupsMeta).forall {
        case (left, right) => isEqual(left, right)
      }
  }

  def isEqual(l: ColumnMeta, r: ColumnMeta): Boolean = {

    l.encoding == r.encoding &&
      l.dictionaryDataLength == r.dictionaryDataLength &&
    l.dictionaryIdSize == r.dictionaryIdSize &&
    isEqual(l.fileStatistics, r.fileStatistics)
  }

  def isEqual(l: ColumnStatistics, r: ColumnStatistics): Boolean = {

    // Equal conditions:
    // Both have no Min Max data
    // Both have Min Max data and they are same.
    (!l.hasNonNullValue && !r.hasNonNullValue) ||
      (l.max.sameElements(r.max) && l.min.sameElements(r.min))
  }


  property("read/write ColumnStatistics") = forAll { (columnStatistics: ColumnStatistics) =>
    val columnStatistics2 = columnStatistics match {
      case ColumnStatistics(bytes) =>
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        ColumnStatistics(in)
    }

    isEqual(columnStatistics, columnStatistics2)
  }

  property("read/write ColumnMeta") = forAll { (columnMeta: ColumnMeta) =>
    val columnMeta2 = columnMeta match {
      case ColumnMeta(bytes) =>
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        ColumnMeta(in)
    }

    isEqual(columnMeta, columnMeta2)
  }

  property("read/write OapDataFileMeta") =
    forAll { (oapDataFileMeta: OapDataFileMetaV1) =>

    val file = new Path(
      new File(tmpDir.getAbsolutePath, "testOapDataFileMeta.meta").getAbsolutePath)
    val fs = file.getFileSystem(conf)
    val output = fs.create(file)
    // Write OapDataFileMeta into file
    oapDataFileMeta.write(output)
    output.close()

    // Read OapDataFileMeta from file
    val fileMeta = new OapDataFileMetaV1().read(fs.open(file), fs.getFileStatus(file).getLen)
    fs.delete(file, false)

    isEqual(fileMeta, oapDataFileMeta)
  }
}

class OapDataFileMetaV1Suite extends SparkFunSuite with Checkers {

  test("Check OapDataFileMeta Read/Write") {
    check(PropertiesAdapter.getProp(new OapDataFileMetaCheck()))
  }
}
