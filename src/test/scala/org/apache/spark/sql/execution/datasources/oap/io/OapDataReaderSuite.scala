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

import java.io.File

import org.apache.hadoop.fs.Path
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OapException, PartitionedFile}
import org.apache.spark.sql.execution.datasources.oap.{INDEX_STAT, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.INDEX_STAT.INDEX_STAT
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class OapDataReaderSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  private var tmpDir: File = _
  private lazy val fs = (new Path(tmpDir.getPath)).getFileSystem(configuration)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tmpDir = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tmpDir)
    } finally {
      super.afterAll()
    }
  }

  // Override afterEach because we don't want to check open streams
  override def beforeEach(): Unit = {}
  override def afterEach(): Unit = {}

  test("OAP data file version read: OAP_DATAFILE_V1") {
    val path = new Path(tmpDir.getPath + "/tmp")
    val outputStream = fs.create(path)
    new OapDataFileMetaV1().write(outputStream)
    outputStream.close()
    assert(OapDataReader.readVersion(
        fs.open(path), fs.getFileStatus(path).getLen) ==
        OapDataFileProperties.DataFileVersion.OAP_DATAFILE_V1)
  }

  test("OAP data file version read: not a valid OAP data file") {
    val FAKE_MAGIC_VERSION = "GCZ1"
    val LENGTH = 4

    val path = new Path(tmpDir.getPath + "/tmp2")
    val outputStream = fs.create(path)
    outputStream.writeBytes(FAKE_MAGIC_VERSION)
    outputStream.writeInt(LENGTH)
    outputStream.close()
    val e = intercept[OapException](
      OapDataReader.readVersion(fs.open(path), fs.getFileStatus(path).getLen))
    assert(e.getMessage == "Not a valid Oap Data File")
  }

  test("OAP data file version read: not supported OAP data file version") {
    val FAKE_MAGIC_VERSION = "OAP9"
    val LENGTH = 4

    val path = new Path(tmpDir.getPath + "/tmp3")
    val outputStream = fs.create(path)
    outputStream.writeBytes(FAKE_MAGIC_VERSION)
    outputStream.writeInt(LENGTH)
    outputStream.close()
    val e = intercept[OapException](
      OapDataReader.readVersion(fs.open(path), fs.getFileStatus(path).getLen))
    assert(e.getMessage == "Not a supported Oap Data File version")
  }

  test("get Data file class for reading: Parquet") {
    val mockOapDataReaderV1 = mock(classOf[OapDataReaderV1])
    assert(
      OapDataReader.getDataFileClassFor(
        OapFileFormat.PARQUET_DATA_FILE_CLASSNAME, mockOapDataReaderV1)
          == OapFileFormat.PARQUET_DATA_FILE_CLASSNAME)
  }

  test("get Data file class for reading: OAP data file V1") {
    val mockOapDataReaderV1 = mock(classOf[OapDataReaderV1])
    assert(
      OapDataReader.getDataFileClassFor(
        OapFileFormat.OAP_DATA_FILE_CLASSNAME, mockOapDataReaderV1)
          == OapFileFormat.OAP_DATA_FILE_V1_CLASSNAME)
  }

  test("get Data file class for reading: undefined data reader class from DataSourceMeta") {
    val whatever = mock(classOf[OapDataReaderV1])
    val WHATEVER = "whatever"
    val e = intercept[OapException](OapDataReader.getDataFileClassFor(WHATEVER, whatever))
    assert(e.getMessage.contains("Undefined data reader class name"))
  }

  test("get Data file class for reading: undefined connection") {
    class OapDataReaderUndefinedConnection extends OapDataReader {
      override def read(file: PartitionedFile): Iterator[InternalRow] = Iterator.empty

      // The two following fields have to be defined by certain versions of OapDataReader for use in
      override def rowsReadByIndex: Option[Long] = None

      // Whatever : )
      override def indexStat: INDEX_STAT = INDEX_STAT.HIT_INDEX
    }
    val mockOapDataReaderUndefined = mock(classOf[OapDataReaderUndefinedConnection])

    val e = intercept[OapException](OapDataReader.getDataFileClassFor(
        OapFileFormat.OAP_DATA_FILE_CLASSNAME, mockOapDataReaderUndefined))
    assert(e.getMessage.contains("Undefined connection"))
  }
}
