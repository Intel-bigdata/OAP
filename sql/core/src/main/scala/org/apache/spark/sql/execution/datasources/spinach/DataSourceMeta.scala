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

import java.io.{DataOutputStream, IOException}
import java.nio.charset.StandardCharsets

import scala.collection.immutable.BitSet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.types.StructType

/**
 * The Spinach meta file is organized in the following format.
 *
 * FileMeta 1        -- 512 bytes
 *     FingerPrint   -- 248 bytes -- The signature of the file.
 *     RecordCount   --   8 bytes -- The record count in the segment.
 *     DataFileName  -- 256 bytes -- The associated data file name. The path is not included.
 * FileMeta 2
 *    .
 *    .
 * FileMeta N
 * IndexMeta 1      -- 512 bytes
 *     Name         -- 255 bytes -- The index name.
 *     indexType    --   1 bytes -- The index type. Sort(0)/ Bitmap Mask(1).
 *     key          -- 256 bytes -- The bit mask for keys.
 * IndexMeta 2
 *    .
 *    .
 * IndexMeta N
 * Schema           -- Variable Length -- The table schema in json format.
 * FileHeader       --  32 bytes
 *     RecordCount  --   8 bytes -- The number of all of the records in the same folder.
 *     DataFileCount--   8 bytes -- The number of the data files.
 *     IndexCount   --   8 bytes -- The number of the index.
 *     Version      --   3 bytes -- Each bytes represents Major, Minor and Revision.
 *     MagicNumber  --   5 bytes -- The magic number of the meta file which is always "FIBER".
 *
 */

private[spinach] case class FileMeta(fingerprint: String, recordCount: Long, dataFileName: String)

private[spinach] case class IndexMeta(name: String, indexType: Byte, key: BitSet)

private[spinach] case class Version(major: Byte, minor: Byte, revision: Byte)

private[spinach] case class FileHeader(
    recordCount: Long,
    dataFileCount: Long,
    indexCount: Long,
    version: Version,
    magicNumber: String)

private[spinach] case class DataSourceMeta(
    fileMetas: Array[FileMeta],
    indexMetas: Array[IndexMeta],
    schema: StructType,
    fileHeader: FileHeader) {

  def open(path: String, context: TaskAttemptContext, requiredIds: Array[Int]): DataFileScanner = {
    // TODO this will be used for integration with SpinachDataReader2
    throw new NotImplementedError("")
  }
}

private[spinach] object DataSourceMeta {

  final val MAGIC_NUMBER = "FIBER"
  final val FILE_HEAD_LEN = 32

  final val FILE_META_START_OFFSET = 0
  final val FILE_META_LENGTH = 512
  final val FILE_META_FINGERPRINT_LENGTH = 248
  final val FILE_META_DATA_FILE_NAME_LENGTH =256

  final val INDEX_META_LENGTH = 512
  final val INDEX_META_NAME_LENGTH = 255
  final val INDEX_META_TYPE_LENGTH = 1
  final val INDEX_META_KEY_LENGTH = 256

  private def readFileHeader(file: FileStatus, in: FSDataInputStream): FileHeader = {
    if (file.getLen < FILE_HEAD_LEN) {
      throw new IOException(s" ${file.getPath} is not a valid Spinach meta file.")
    }

    in.seek(file.getLen - FILE_HEAD_LEN)
    val recordCount = in.readLong()
    val dataFileCount = in.readLong()
    val indexCount = in.readLong()
    val version = Version(in.readByte(), in.readByte(), in.readByte())
    val buffer = new Array[Byte](MAGIC_NUMBER.length)
    in.readFully(buffer)
    val magicNumber = new String(buffer, StandardCharsets.UTF_8)
    if (magicNumber != MAGIC_NUMBER) {
      throw new IOException(s" ${file.getPath} is not a valid Spinach meta file.")
    }

    FileHeader(recordCount, dataFileCount, indexCount, version, magicNumber)
  }

  private def writeFileHeader(fileHeader: FileHeader, out: DataOutputStream): Unit = {
    out.writeLong(fileHeader.recordCount)
    out.writeLong(fileHeader.dataFileCount)
    out.writeLong(fileHeader.indexCount)
    out.writeByte(fileHeader.version.major)
    out.writeByte(fileHeader.version.minor)
    out.writeByte(fileHeader.version.revision)
    out.write(MAGIC_NUMBER.getBytes(StandardCharsets.UTF_8))
  }

  private def readFileMetas(fileHeader: FileHeader, in: FSDataInputStream): Array[FileMeta] = {
    val dataFileCount = fileHeader.dataFileCount.toInt
    val fileMetas = new Array[FileMeta](dataFileCount)
    var readPos = FILE_META_START_OFFSET

    for (i <- 0 until dataFileCount) {
      in.seek(readPos)
      val fingerprint = in.readUTF()
      readPos += FILE_META_FINGERPRINT_LENGTH

      in.seek(readPos)
      val recordCount = in.readLong()
      readPos += 8

      val dataFileName = in.readUTF()
      readPos += FILE_META_DATA_FILE_NAME_LENGTH

      fileMetas(i) = FileMeta(fingerprint, recordCount, dataFileName)
    }
    fileMetas
  }

  private def writeFileMetas(fileMetas: Array[FileMeta], out: DataOutputStream): Unit = {
    for (fileMeta <- fileMetas) {
      // Write the fingerprint
      writeString(fileMeta.fingerprint, FILE_META_FINGERPRINT_LENGTH, out)

      // Write the record count
      out.writeLong(fileMeta.recordCount)

      // Write the associated data file name
      writeString(fileMeta.dataFileName, FILE_META_DATA_FILE_NAME_LENGTH, out)
    }
  }

  private def readIndexMetas(fileHeader: FileHeader, in: FSDataInputStream): Array[IndexMeta] = {
    val indexCount = fileHeader.indexCount.toInt
    val indexMetas = new Array[IndexMeta](indexCount)
    var readPos = FILE_META_START_OFFSET + FILE_META_LENGTH * fileHeader.dataFileCount

    for (i <- 0 until indexCount) {
      in.seek(readPos)
      val name = in.readUTF()
      readPos += INDEX_META_NAME_LENGTH

      in.seek(readPos)
      val indexType = in.readByte()
      readPos += 1

      val bitMask = new Array[Long](INDEX_META_KEY_LENGTH / 8)
      for (j <- 0 until INDEX_META_KEY_LENGTH / 8) {
        bitMask(j) = in.readLong()
      }
      val keyBits = BitSet.fromBitMask(bitMask)
      readPos += INDEX_META_KEY_LENGTH

      indexMetas(i) = IndexMeta(name, indexType, keyBits)
    }
    indexMetas
  }

  private def writeIndexMetas(indexMetas: Array[IndexMeta], out: DataOutputStream): Unit = {
    for (indexMeta <- indexMetas) {
      // Write the index name
      writeString(indexMeta.name, INDEX_META_NAME_LENGTH, out)

      // Write the index type
      out.writeByte(indexMeta.indexType)

      // Write the bit mask of keys
      writeBitset(indexMeta.key, INDEX_META_KEY_LENGTH, out)
    }
  }

  private def readSchema(fileHeader: FileHeader, in: FSDataInputStream) : StructType = {
    in.seek(FILE_META_START_OFFSET + FILE_META_LENGTH * fileHeader.dataFileCount +
      INDEX_META_LENGTH * fileHeader.indexCount)
    StructType.fromString(in.readUTF())
  }

  private def writeSchema(schema: StructType, out: DataOutputStream): Unit = {
    out.writeUTF(schema.json)
  }

  private def writeString(value: String, totalSizeToWrite: Int, out: DataOutputStream): Unit = {
    val sizeBefore = out.size
    out.writeUTF(value)
    val sizeWritten = out.size - sizeBefore
    val remaining = totalSizeToWrite - sizeWritten
    assert(remaining >= 0,
      s"Failed to write $value as it exceeds the max allowed $totalSizeToWrite bytes.")
    for (i <- 0 until remaining) {
      out.writeByte(0)
    }
  }

  private def writeBitset(value: BitSet, totalSizeToWrite: Int, out: DataOutputStream): Unit = {
    val sizeBefore = out.size
    value.toBitMask.foreach(out.writeLong)
    val sizeWritten = out.size - sizeBefore
    val remaining = totalSizeToWrite - sizeWritten
    assert(remaining >= 0,
      s"Failed to write $value as it exceeds the max allowed $totalSizeToWrite bytes.")
    for (i <- 0 until remaining) {
      out.writeByte(0)
    }
  }

  def initialize(path: Path, jobConf: Configuration): DataSourceMeta = {
    val fs = path.getFileSystem(jobConf)
    val file = fs.getFileStatus(path)
    val in = fs.open(path)

    val fileHeader = readFileHeader(file, in)
    val fileMetas = readFileMetas(fileHeader, in)
    val indexMetas = readIndexMetas(fileHeader, in)
    val schema = readSchema(fileHeader, in)
    in.close()
    DataSourceMeta(fileMetas, indexMetas, schema, fileHeader)
  }

  def write(
      path: Path,
      jobConf: Configuration,
      meta: DataSourceMeta,
      deleteIfExit: Boolean = true): Unit = {
    val fs = path.getFileSystem(jobConf)
    if (fs.exists(path)) {
      if (deleteIfExit) {
        fs.delete(path, true)
      } else {
        throw new FileAlreadyExistsException(s"File $path already exists.")
      }
    }
    val out = fs.create(path)
    writeFileMetas(meta.fileMetas, out)
    writeIndexMetas(meta.indexMetas, out)
    writeSchema(meta.schema, out)
    writeFileHeader(meta.fileHeader, out)
    out.close()
  }
}
