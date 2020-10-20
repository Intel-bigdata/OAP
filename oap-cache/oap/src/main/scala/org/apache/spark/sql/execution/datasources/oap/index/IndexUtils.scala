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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.OutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.{FileFormatWriter, FileIndex, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.OapIndexWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.oap.IndexMeta
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.execution.datasources.oap.io.{BytesCompressor, BytesDecompressor, IndexFile}
import org.apache.spark.sql.execution.datasources.orc.ReadOnlyNativeOrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ReadOnlyParquetFileFormat
import org.apache.spark.sql.hive.orc.ReadOnlyOrcFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

/**
 * Utils for Index read/write
 */
private[oap] object IndexUtils extends  Logging {

  def readVersion(fileReader: IndexFileReader): Option[Int] = {
    val magicBytes = fileReader.read(0, IndexFile.VERSION_LENGTH)
    deserializeVersion(magicBytes)
  }

  def serializeVersion(versionNum: Int): Array[Byte] = {
    assert(versionNum <= 65535)
    IndexFile.VERSION_PREFIX.getBytes("UTF-8") ++
      Array((versionNum >> 8).toByte, (versionNum & 0xFF).toByte)
  }

  def deserializeVersion(bytes: Array[Byte]): Option[Int] = {
    val prefix = IndexFile.VERSION_PREFIX.getBytes("UTF-8")
    val versionPos = bytes.length - 2
    assert(bytes.length == prefix.length + 2)
    if (bytes.slice(0, prefix.length) sameElements prefix) {
      val version = bytes(versionPos) << 8 | bytes(versionPos + 1)
      Some(version)
    } else {
      None
    }
  }

  def writeHead(writer: OutputStream, versionNum: Int): Int = {
    val versionData = serializeVersion(versionNum)
    assert(versionData.length == IndexFile.VERSION_LENGTH)
    writer.write(versionData)
    IndexFile.VERSION_LENGTH
  }

  /**
   * Get the data file name as the index file name. For example the data file is
   * "/tmp/part-00000-df7c3ca8-560f-4089-a0b1-58ab817bc2c3.snappy.parquet",
   * the index file name is part-00000-df7c3ca8-560f-4089-a0b1-58ab817bc2c3.snappy
   * @param dataFile the data file
   * @return the index file name
   */
  private def getIndexFileNameFromDatafile (dataFile: Path): String = {
    val dataFileName = dataFile.getName
    val pos = dataFileName.lastIndexOf(".")
    if (pos > 0) {
      dataFileName.substring(0, pos)
    } else {
      dataFileName
    }
  }

  /**
   * Get the index file path based on the configuration of OapConf.OAP_INDEX_DIRECTORY,
   * the data file, index name and the created index time.
   * Here the data file is to generated the index file name and get the parent path of data file.
   * If the OapConf.OAP_INDEX_DIRECTORY is "", the index file path is generated based
   * on the the parent path of dataFile + index file name; otherwise
   * the index file path is the value of OapConf.OAP_INDEX_DIRECTORY +
   * the parent path of dataFile +index file name
   *
   * @param conf     the configuration to get the value of OapConf.OAP_INDEX_DIRECTORY
   * @param dataFile the path of the data file
   * @param name     the name of the index
   * @param time     the time of creating index
   * @return the generated index path
   */
  def getIndexFilePath(
      conf: Configuration, dataFile: Path, name: String, time: String): Path = {
    import OapFileFormat._
    val indexDirectory = conf.get(
      OapConf.OAP_INDEX_DIRECTORY.key, OapConf.OAP_INDEX_DIRECTORY.defaultValueString)
    val indexFileName = getIndexFileNameFromDatafile(dataFile)
    if (indexDirectory.trim != "") {
      new Path(
        indexDirectory + "/" + Path.getPathWithoutSchemeAndAuthority(dataFile.getParent),
        s"${"." + indexFileName + "." + time + "." + name + OAP_INDEX_EXTENSION}")
    } else {
      new Path(
        dataFile.getParent,
        s"${"." + indexFileName + "." + time + "." + name + OAP_INDEX_EXTENSION}")
    }
  }

  def writeFloat(out: OutputStream, v: Float): Unit =
    writeInt(out, java.lang.Float.floatToIntBits(v))

  def writeDouble(out: OutputStream, v: Double): Unit =
    writeLong(out, java.lang.Double.doubleToLongBits(v))

  def writeBoolean(out: OutputStream, v: Boolean): Unit = out.write(if (v) 1 else 0)

  def writeByte(out: OutputStream, v: Int): Unit = out.write(v)

  def writeBytes(out: OutputStream, b: Array[Byte]): Unit = out.write(b)

  def writeShort(out: OutputStream, v: Int): Unit = {
    out.write(v >>> 0 & 0XFF)
    out.write(v >>> 8 & 0xFF)
  }

  def toBytes(v: Int): Array[Byte] = {
    Array(0, 8, 16, 24).map(shift => ((v >>> shift) & 0XFF).toByte)
  }

  def writeInt(out: OutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
  }

  def writeLong(out: OutputStream, v: Long): Unit = {
    out.write((v >>>  0).toInt & 0xFF)
    out.write((v >>>  8).toInt & 0xFF)
    out.write((v >>> 16).toInt & 0xFF)
    out.write((v >>> 24).toInt & 0xFF)
    out.write((v >>> 32).toInt & 0xFF)
    out.write((v >>> 40).toInt & 0xFF)
    out.write((v >>> 48).toInt & 0xFF)
    out.write((v >>> 56).toInt & 0xFF)
  }

  /**
   * Generate the temp index file path based on the configuration of OapConf.OAP_INDEX_DIRECTORY,
   * the inputFile, outputPath passed by FileFormatWriter,
   * attemptPath generated by FileFormatWriter.
   * Here the inputFile is to generated the index file name and get the parent path of inputFile.
   * If the OapConf.OAP_INDEX_DIRECTORY is "", the index file path is the the parent path of
   * inputFile + index file name; otherwise
   * the index file path is the value of OapConf.OAP_INDEX_DIRECTORY +
   * the parent path of inputFile +index file name
   *
   * @param conf        the configuration to get the value of OapConf.OAP_INDEX_DIRECTORY
   * @param inputFile   the input data file path which contain the partition path
   * @param outputPath  the outputPath passed by FileFormatWriter.getOutputPath
   * @param attemptPath the temp file path generated by FileFormatWriter
   * @param extension   the extension name of the index file
   * @return the index path
   */
  def generateTempIndexFilePath(
      conf: Configuration, inputFile: String,
      outputPath: Path, attemptPath: String, extension: String): Path = {
    val inputFilePath = new Path(inputFile)
    val indexFileName = getIndexFileNameFromDatafile(inputFilePath)
    val indexDirectory = conf.get(OapConf.OAP_INDEX_DIRECTORY.key,
      OapConf.OAP_INDEX_DIRECTORY.defaultValueString)
    if (indexDirectory.trim != "") {
      // here the outputPath = indexDirectory + tablePath or
      // indexDirectory + tablePath+ partitionPath
      // we should also remove the schema of indexDirectory when get the tablePath
      // to avoid the wrong tablePath when the indexDirectory contain schema. file:/tmp
      // For example: indexDirectory = file:/tmp outputPath = file:/tmp/tablePath
      val tablePath =
      Path.getPathWithoutSchemeAndAuthority(outputPath).toString.replaceFirst(
        Path.getPathWithoutSchemeAndAuthority(new Path(indexDirectory)).toString, "")
      val partitionPath =
        Path.getPathWithoutSchemeAndAuthority(
          inputFilePath.getParent).toString.replaceFirst(tablePath.toString, "")
      new Path(new Path(attemptPath).getParent.toString + "/"
        + partitionPath + "/." + indexFileName + extension)
    } else {
      new Path(inputFilePath.getParent.toString.replace(
        outputPath.toString, new Path(attemptPath).getParent.toString),
        "." + indexFileName + extension)
    }
  }

  /**
   * Generate the outPutPath based on OapConf.OAP_INDEX_DIRECTORY and the data path,
   * here the dataPath may be the table path + partition path if the fileIndex.rootPaths
   * has 1 item, or the table path
   * @param fileIndex [[FileIndex]] of a relation
   * @param conf the configuration to get the value of OapConf.OAP_INDEX_DIRECTORY
   * @return the outPutPath to save the job temporary data
   */
  def getOutputPathBasedOnConf(fileIndex: FileIndex, conf: RuntimeConfig): Path = {
    def getTableBaseDir(path: Path, times: Int): Path = {
      if (times > 0) getTableBaseDir(path.getParent, times - 1)
      else path
    }
    val paths = fileIndex.rootPaths
    assert(paths.nonEmpty, "Expected at least one path of fileIndex.rootPaths, but no value")
    val dataPath = paths.length match {
      case 1 => paths.head
      case _ => getTableBaseDir(paths.head, fileIndex.partitionSchema.length)
    }

    val indexDirectory = conf.get(OapConf.OAP_INDEX_DIRECTORY.key)
    if (indexDirectory.trim != "") {
      new Path (
        indexDirectory + Path.getPathWithoutSchemeAndAuthority(dataPath).toString)
    } else {
      dataPath
    }
  }

  def getOutputPathBasedOnConf(
      partitionDirs: Seq[PartitionDirectory], fileIndex: FileIndex, conf: RuntimeConfig): Path = {

    def getTableBaseDir(path: Path, times: Int): Path = {
      if (times > 0) getTableBaseDir(path.getParent, times - 1)
      else path
    }

    val prtitionLength = fileIndex.partitionSchema.length
    val baseDirs = partitionDirs
      .filter(_.files.nonEmpty).map(dir => dir.files.head.getPath.getParent)
      .map(getTableBaseDir(_, prtitionLength)).toSet

    val dataPath = if (baseDirs.isEmpty) {
      getOutputPathBasedOnConf(fileIndex, conf)
    } else if (baseDirs.size == 1) {
      baseDirs.head
    } else {
      throw new UnsupportedOperationException("Not support multi data base dir now")
    }
    logWarning(s"data path = $dataPath")

    val indexDirectory = conf.get(OapConf.OAP_INDEX_DIRECTORY.key)
    if (indexDirectory.trim != "") {
      new Path (
        indexDirectory + Path.getPathWithoutSchemeAndAuthority(dataPath).toString)
    } else {
      dataPath
    }
  }

  val INT_SIZE = 4
  val LONG_SIZE = 8

  /**
   * Constrain: keys.last >= candidate must be true. This is guaranteed
   * by [[BTreeIndexRecordReader.findNodeIdx]]
   * @return the first key >= candidate. (keys.last >= candidate makes this always possible)
   */
  def binarySearch(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = start
    var e = length - 1
    var found = false
    var m = s
    while (s <= e && !found) {
      assert(s + e >= 0, "too large array size caused overflow")
      m = (s + e) / 2
      val cmp = compare(candidate, keys(m))
      if (cmp == 0) {
        found = true
      } else if (cmp > 0) {
        s = m + 1
      } else {
        e = m - 1
      }
      if (!found) {
        m = s
      }
    }
    (m, found)
  }

  def binarySearchForStart(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = start + 1
    var e = length - 1
    lazy val initCmp = compare(candidate, keys(0))
    if (length <= 0 || initCmp <= 0) {
      return (0, length > 0 && initCmp == 0)
    }
    var found = false
    var m = s
    while (s <= e && !found) {
      assert(s + e >= 0, "too large array size caused overflow")
      m = (s + e) / 2
      val cmp = compare(candidate, keys(m))
      val marginCmp = compare(candidate, keys(m - 1))
      if (cmp == 0 && marginCmp > 0) found = true
      else if (cmp > 0) s = m + 1
      else e = m - 1
    }
    if (!found) m = s
    (m, found)
  }

  def binarySearchForEnd(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = start
    var e = length - 2
    lazy val initCmp = compare(candidate, keys(length - 1))
    if (length <= 0 || compare(candidate, keys(0)) < 0) {
      (-1, false)
    } else if (initCmp > 0) {
      (length, false)
    } else if (initCmp == 0) {
      (length - 1, true)
    } else {
      var (m, found) = (s, false)
      while (s <= e && !found) {
        assert(s + e >= 0, "too large array size caused overflow")
        m = (s + e) / 2
        val cmp = compare(candidate, keys(m))
        val marginCmp = compare(candidate, keys(m + 1))
        if (cmp == 0 && marginCmp < 0) found = true
        else if (cmp < 0) e = m - 1
        else s = m + 1
      }
      if (!found) m = s
      (m, found)
    }
  }

  private val CODEC_MAGIC: Array[Byte] = "CODEC".getBytes("UTF-8")

  def compressIndexData(compressor: BytesCompressor, bytes: Array[Byte]): Array[Byte] = {
    CODEC_MAGIC ++ toBytes(bytes.length) ++ compressor.compress(bytes)
  }

  def decompressIndexData(decompressor: BytesDecompressor, bytes: Array[Byte]): Array[Byte] = {
    if (CODEC_MAGIC.sameElements(bytes.slice(0, CODEC_MAGIC.length))) {
      val length = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + CODEC_MAGIC.length)
      val decompressedBytes =
        decompressor.decompress(bytes.slice(CODEC_MAGIC.length + INT_SIZE, bytes.length), length)
      decompressedBytes
    } else {
      bytes
    }
  }

  def extractInfoFromPlan(sparkSession: SparkSession, optimized: LogicalPlan)
    : (FileIndex, StructType, String, Option[CatalogTable], LogicalPlan) = {
    val (fileCatalog, schema, readerClassName, identifier, relation) = optimized match {
      case LogicalRelation(
          _fsRelation @ HadoopFsRelation(f, _, s, _, _: ParquetFileFormat, _),
          attributes, id, _) =>
        if (!(sparkSession.conf.get(OapConf.OAP_PARQUET_ENABLED) &&
          sparkSession.conf.get(OapConf.OAP_PARQUET_ENABLE))) {
          throw new OapException(s"turn on ${
            OapConf.OAP_PARQUET_ENABLED.key
          } to allow index operation on parquet files")
        }
        // Use ReadOnlyParquetFileFormat instead of ParquetFileFormat because of
        // ReadOnlyParquetFileFormat.isSplitable always return false.
        val fsRelation = _fsRelation.copy(
          fileFormat = new ReadOnlyParquetFileFormat(),
          options = _fsRelation.options)(_fsRelation.sparkSession)
        val logical = LogicalRelation(fsRelation, attributes, id, isStreaming = false)
        (f, s, OapFileFormat.PARQUET_DATA_FILE_CLASSNAME, id, logical)
      case LogicalRelation(
          _fsRelation @ HadoopFsRelation(f, _, s, _, format, _), attributes, id, _)
        if format.isInstanceOf[org.apache.spark.sql.hive.orc.OrcFileFormat] ||
          format.isInstanceOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat] =>
        if (!(sparkSession.conf.get(OapConf.OAP_ORC_ENABLED) &&
          sparkSession.conf.get(OapConf.OAP_ORC_ENABLE)))
        {
          throw new OapException(s"turn on ${
            OapConf.OAP_ORC_ENABLED.key
          } to allow index building on orc files")
        }
        // ReadOnlyOrcFileFormat and ReadOnlyNativeOrcFileFormat don't support splitable.
        // ReadOnlyOrcFileFormat is for hive orc.
        // ReadOnlyNativeOrcFileFormat is for native orc introduced in Spark 2.3.
        val fsRelation = format match {
          case _: org.apache.spark.sql.hive.orc.OrcFileFormat =>
            _fsRelation.copy(fileFormat = new ReadOnlyOrcFileFormat(),
              options = _fsRelation.options)(_fsRelation.sparkSession)
          case _: org.apache.spark.sql.execution.datasources.orc.OrcFileFormat =>
            _fsRelation.copy(fileFormat = new ReadOnlyNativeOrcFileFormat(),
              options = _fsRelation.options)(_fsRelation.sparkSession)
        }
        val logical = LogicalRelation(fsRelation, attributes, id, isStreaming = false)
        (f, s, OapFileFormat.ORC_DATA_FILE_CLASSNAME, id, logical)
      case other =>
        throw new OapException(s"We don't support index operation for " +
          s"${other.simpleString(SQLConf.get.maxToStringFields)}")
    }
    (fileCatalog, schema, readerClassName, identifier, relation)
  }

  def buildPartitionsFilter(partitions: Seq[PartitionDirectory], schema: StructType): String = {
    partitions.map{ p =>
      (0 until p.values.numFields).
        map(i => (schema.fields(i).name, p.values.get(i, schema.fields(i).dataType))).
        map{ case (k, v) => s"$k='$v'" }.reduce(_ + " AND " + _)
    }.map("(" + _ + ")").reduce(_ + " OR " + _)
  }

  def buildPartitionIndex(
    relation: LogicalPlan,
    sparkSession: SparkSession,
    partitions: Seq[PartitionDirectory],
    outPutPath: Path,
    partitionSchema: StructType,
    indexColumns: Seq[IndexColumn],
    indexType: OapIndexType,
    indexMeta: IndexMeta): Seq[IndexBuildResult] = {
    val projectList = indexColumns.map { indexColumn =>
      relation.output.find(p => p.name == indexColumn.columnName).get.withMetadata(
        new MetadataBuilder().putBoolean("isAscending", indexColumn.isAscending).build())
    }

    var ds = Dataset.ofRows(sparkSession, Project(projectList, relation))
    if (partitionSchema.nonEmpty) {
      val disjunctivePartitionsFilter = buildPartitionsFilter(partitions, partitionSchema)
      ds = ds.filter(disjunctivePartitionsFilter)
    }

    assert(outPutPath != null, "Expected exactly one path to be specified, but no value")

    val configuration = sparkSession.sessionState.newHadoopConf()
    val qualifiedOutputPath = {
      val fs = outPutPath.getFileSystem(configuration)
      outPutPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    }

    val committer = FileCommitProtocol.instantiate(
      classOf[OapIndexCommitProtocol].getCanonicalName,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outPutPath.toUri.toString,
      false)

    val options = Map(
      "indexName" -> indexMeta.name,
      "indexTime" -> indexMeta.time,
      "isAppend" -> "true",
      "indexType" -> indexType.toString
    )

    val statsTrackers = new OapIndexWriteJobStatsTracker

    FileFormatWriter.write(
      sparkSession = sparkSession,
      ds.queryExecution.executedPlan,
      fileFormat = new OapIndexFileFormat,
      committer = committer,
      outputSpec = FileFormatWriter.OutputSpec(
        qualifiedOutputPath.toUri.toString, Map.empty, ds.queryExecution.analyzed.output),
      hadoopConf = configuration,
      Seq.empty, // partitionColumns
      bucketSpec = None,
      statsTrackers = Seq(statsTrackers),
      options = options)

    statsTrackers.indexBuildResults
  }
}
