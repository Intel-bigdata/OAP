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

package org.apache.spark.sql.execution.datasources.spinach

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.{CompressionCodec, Encoding}
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalatest.prop.Checkers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.spinach.io.{RowGroupMeta, SpinachDataFileHandle}
import org.apache.spark.util.Utils

class SpinachDataFileHandleCheck extends Properties("SpinachDataFileHandle") {

  private val tmpDir = Utils.createTempDir()
  private val conf = new Configuration()

  private lazy val genSpinachDataFileHandle: Gen[SpinachDataFileHandle] = {
    // TODO: [linhong] Need determine the range of each value.
    for { rowGroupCount <- Gen.choose[Int](1, 1000)
      defaultRowCount <- Gen.choose[Int](1, 1048576)
      fieldCount <- Gen.choose[Int](1, 300)
      lastRowCount <- Gen.choose[Int](1, defaultRowCount)
      fiberLens <- Gen.listOfN(fieldCount, Gen.choose[Int](0, 1048576))
      uncompressedFiberLens <- Gen.listOfN(fieldCount, Gen.choose[Int](0, 1048576))
      dictionaryDataLens <- Gen.listOfN(fieldCount, Gen.choose[Int](0, 1048576))
      dictionaryIdSizes <- Gen.listOfN(fieldCount, Gen.choose[Int](0, 1048576))
      encodings <- Gen.listOfN(fieldCount,
        Gen.oneOf(Encoding.PLAIN,
          Encoding.PLAIN_DICTIONARY,
          Encoding.DELTA_BYTE_ARRAY,
          Encoding.RLE))
      codec <- Gen.oneOf(CompressionCodec.GZIP,
        CompressionCodec.LZO,
        CompressionCodec.GZIP,
        CompressionCodec.UNCOMPRESSED)
    } yield generateSpinachDataFileHandle(rowGroupCount,
      defaultRowCount,
      fieldCount,
      lastRowCount,
      fiberLens.toArray,
      uncompressedFiberLens.toArray,
      dictionaryDataLens.toArray,
      dictionaryIdSizes.toArray,
      encodings.toArray,
      codec)
  }

  implicit lazy val arbSpinachDataFileHandle: Arbitrary[SpinachDataFileHandle] = {
    Arbitrary(genSpinachDataFileHandle)
  }

  private def generateSpinachDataFileHandle(rowGroupCount: Int,
                                            defaultRowCount: Int,
                                            fieldCount: Int,
                                            lastRowCount: Int,
                                            fiberLens: Array[Int],
                                            uncompressedFiberLens: Array[Int],
                                            dictionaryDataLens: Array[Int],
                                            dictionaryIdSizes: Array[Int],
                                            encodings: Array[Encoding],
                                            codec: CompressionCodec): SpinachDataFileHandle = {

    val rowGroupMetaArray = new Array[RowGroupMeta](rowGroupCount)
    rowGroupMetaArray.indices.foreach(
      rowGroupMetaArray(_) = new RowGroupMeta()
        .withNewStart(0)
        .withNewEnd(100)
        .withNewFiberLens(fiberLens)
        .withNewUncompressedFiberLens(uncompressedFiberLens)
    )

    val spinachDataFileHandle = new SpinachDataFileHandle(
      rowCountInEachGroup = defaultRowCount,
      fieldCount = fieldCount,
      codec = codec
    )

    spinachDataFileHandle
      .withGroupCount(rowGroupCount)
      .withRowCountInLastGroup(lastRowCount)
      .withEncodings(encodings)
      .withDictionaryDataLens(dictionaryDataLens)
      .withDictionaryIdSizes(dictionaryIdSizes)

    rowGroupMetaArray.foreach(spinachDataFileHandle.appendRowGroupMeta)

    spinachDataFileHandle
  }

  private def compareSpinachFileHandle(
                                       meta1: SpinachDataFileHandle,
                                       meta2: SpinachDataFileHandle): Unit = {

    assert(meta1.rowCountInEachGroup == meta2.rowCountInEachGroup)
    assert(meta1.rowCountInLastGroup == meta2.rowCountInLastGroup)
    assert(meta1.groupCount == meta2.groupCount)
    assert(meta1.fieldCount == meta2.fieldCount)
    assert(meta1.codec == meta2.codec)
    assert(meta1.encodings.length == meta2.encodings.length)
    assert(meta1.dictionaryDataLens.length == meta2.dictionaryDataLens.length)
    assert(meta1.dictionaryIdSizes.length == meta2.dictionaryIdSizes.length)
    assert(meta1.rowGroupsMeta.length == meta2.rowGroupsMeta.length)

    meta1.encodings.zip(meta2.encodings).foreach{
      case (v1, v2) => assert(v1 == v2)
    }
    meta1.dictionaryDataLens.zip(meta2.dictionaryDataLens).foreach{
      case (v1, v2) => assert(v1 == v2)
    }
    meta1.dictionaryIdSizes.zip(meta2.dictionaryIdSizes).foreach{
      case (v1, v2) => assert(v1 == v2)
    }

    meta1.rowGroupsMeta.zip(meta2.rowGroupsMeta).foreach{
      case (v1, v2) =>
        assert(v1.start == v2.start)
        assert(v1.end == v2.end)
        v1.fiberLens.zip(v2.fiberLens).foreach{
          case (vv1, vv2) => assert(vv1 == vv2)
        }
        v1.fiberUncompressedLens.zip(v2.fiberUncompressedLens).foreach{
          case (vv1, vv2) => assert(vv1 == vv2)
        }
    }
  }

  property("write/read") = forAll { (spinachDataFileHandle: SpinachDataFileHandle) =>

    val file = new Path(
      new File(tmpDir.getAbsolutePath, "testSpinachDataFileHandle.meta").getAbsolutePath)
    val fs = file.getFileSystem(conf)
    val output = fs.create(file)
    // Write SpinachDataFileHandle into file
    spinachDataFileHandle.write(output)
    output.close()

    val groupCount = spinachDataFileHandle.groupCount
    val fieldCount = spinachDataFileHandle.fieldCount
    val fileLen = 12 * fieldCount + 16 * groupCount + 8 * fieldCount * groupCount + 20
    assert(fileLen == fs.getFileStatus(file).getLen)

    // Read SpinachDataFileHandle from file
    val fileHandle = new SpinachDataFileHandle().read(fs.open(file), fs.getFileStatus(file).getLen)

    compareSpinachFileHandle(fileHandle, spinachDataFileHandle)
    fs.delete(file, false)
  }
}

class SpinachDataFileHandleSuite extends SparkFunSuite with Checkers {

  test("Check SpinachDataFileHandle Read/Write") {
    check(new SpinachDataFileHandleCheck)
  }
}
