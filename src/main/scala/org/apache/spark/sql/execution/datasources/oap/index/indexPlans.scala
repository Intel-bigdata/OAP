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

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.utils.OapUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ReadOnlyParquetFileFormat}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.oap.rpc.OapMessages.CacheDrop
import org.apache.spark.sql.types._


/**
 * Creates an index for table on indexColumns
 */
case class CreateIndexCommand(
    indexName: String,
    table: TableIdentifier,
    indexColumns: Array[IndexColumn],
    allowExists: Boolean,
    indexType: OapIndexType,
    partitionSpec: Option[TablePartitionSpec]) extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val qe = sparkSession.sessionState.executePlan(UnresolvedRelation(table))
    qe.assertAnalyzed()
    val optimized = qe.optimizedPlan

    val (fileCatalog, schema, readerClassName, identifier, relation) = optimized match {
      case LogicalRelation(
      _fsRelation @ HadoopFsRelation(f, _, s, _, _: OapFileFormat, _), _, id) =>
        (f, s, OapFileFormat.OAP_DATA_FILE_CLASSNAME, id, optimized)
      case LogicalRelation(
      _fsRelation @ HadoopFsRelation(f, _, s, _, format: ParquetFileFormat, _), attributes, id) =>
        if (!sparkSession.conf.get(OapConf.OAP_PARQUET_ENABLED)) {
          throw new OapException(s"turn on ${
            OapConf.OAP_PARQUET_ENABLED.key} to allow index building on parquet files")
        }
        // Use ReadOnlyParquetFileFormat instead of ParquetFileFormat because of
        // ReadOnlyParquetFileFormat.isSplitable always return false.
        val fsRelation = _fsRelation.copy(
          fileFormat = new ReadOnlyParquetFileFormat(),
          options = _fsRelation.options)(_fsRelation.sparkSession)
        val logical = LogicalRelation(fsRelation, attributes, id)
        (f, s, OapFileFormat.PARQUET_DATA_FILE_CLASSNAME, id, logical)
      case other =>
        throw new OapException(s"We don't support index building for ${other.simpleString}")
    }

    logInfo(s"Creating index $indexName")
    val configuration = sparkSession.sessionState.newHadoopConf()
    val partitions = OapUtils.getPartitions(fileCatalog, partitionSpec)
    // TODO currently we ignore empty partitions, so each partition may have different indexes,
    // this may impact index updating. It may also fail index existence check. Should put index
    // info at table level also.
    val time = System.currentTimeMillis().toHexString
    val bAndP = partitions.filter(_.files.nonEmpty).map(p => {
      val metaBuilder = new DataSourceMetaBuilder()
      val parent = p.files.head.getPath.getParent
      // TODO get `fs` outside of map() to boost
      val fs = parent.getFileSystem(configuration)
      val existOld = fs.exists(new Path(parent, OapFileFormat.OAP_META_FILE))
      if (existOld) {
        val m = OapUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, parent)
        assert(m.nonEmpty)
        val oldMeta = m.get
        val existsIndexes = oldMeta.indexMetas
        val existsData = oldMeta.fileMetas
        if (existsIndexes.exists(_.name == indexName)) {
          if (!allowExists) {
            throw new AnalysisException(
              s"""Index $indexName exists on ${identifier.getOrElse(parent)}""")
          } else {
            logWarning(s"Dup index name $indexName")
            return Nil
          }
        }
        if (existsData != null) {
          existsData.foreach(metaBuilder.addFileMeta)
        }
        if (existsIndexes != null) {
          existsIndexes.filter(_.name != indexName).foreach(metaBuilder.addIndexMeta)
        }
        metaBuilder.withNewSchema(oldMeta.schema)
      } else {
        metaBuilder.withNewSchema(schema)
      }

      indexType match {
        case BTreeIndexType =>
          val entries = indexColumns.map(c => {
            val dir = if (c.isAscending) Ascending else Descending
            BTreeIndexEntry(schema.map(_.name).toIndexedSeq.indexOf(c.columnName), dir)
          })
          metaBuilder.addIndexMeta(new IndexMeta(indexName, time, BTreeIndex(entries)))
        case BitMapIndexType =>
          // Currently OAP index type supports the column with one single field.
          if (indexColumns.length != 1) {
            throw new OapException("BitMapIndexType only supports one single column")
          }
          val entries = indexColumns.map(col =>
            schema.map(_.name).toIndexedSeq.indexOf(col.columnName))
          metaBuilder.addIndexMeta(new IndexMeta(indexName, time, BitMapIndex(entries)))
        case _ =>
          sys.error(s"Not supported index type $indexType")
      }

      // we cannot build meta for those without oap meta data
      metaBuilder.withNewDataReaderClassName(readerClassName)
      // when p.files is nonEmpty but no oap meta, it means the relation is in parquet
      // (else it is Oap empty partition, we won't create meta for them).
      // For Parquet, we only use Oap meta to track schema and reader class, as well as
      // `IndexMeta`s that must be empty at the moment, so `FileMeta`s are ok to leave empty.
      // p.files.foreach(f => builder.addFileMeta(FileMeta("", 0, f.getPath.toString)))
      (metaBuilder, parent, existOld)
    })

    val projectList = indexColumns.map { indexColumn =>
      relation.output.find(p => p.name == indexColumn.columnName).get.withMetadata(
        new MetadataBuilder().putBoolean("isAscending", indexColumn.isAscending).build())
    }

    var ds = Dataset.ofRows(sparkSession, Project(projectList, relation))
    partitionSpec.getOrElse(Map.empty).foreach { case (k, v) =>
      ds = ds.filter(s"$k='$v'")
    }

    val outPutPath = OapUtils.getOutPutPath(fileCatalog)
    assert(outPutPath != null, "Expected exactly one path to be specified, but no value")

    val qualifiedOutputPath = {
      val fs = outPutPath.getFileSystem(configuration)
      outPutPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    }

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outPutPath.toUri.getPath,
      isAppend = false)

    val options = Map(
      "indexName" -> indexName,
      "indexTime" -> time,
      "isAppend" -> "true",
      "indexType" -> indexType.toString
    )

    val retVal = FileFormatWriter.write(
      sparkSession = sparkSession,
      queryExecution = ds.queryExecution,
      fileFormat = new OapIndexFileFormat,
      committer = committer,
      outputSpec = FileFormatWriter.OutputSpec(
        qualifiedOutputPath.toUri.getPath, Map.empty),
      hadoopConf = configuration,
      partitionColumns = Seq.empty,
      bucketSpec = Option.empty,
      refreshFunction = _ => Unit,
      options = options).asInstanceOf[Seq[Seq[IndexBuildResult]]]

    val retMap = retVal.flatten.groupBy(_.parent)
    bAndP.foreach(bp =>
      retMap.getOrElse(bp._2.toString, Nil).foreach(r =>
        if (!bp._3) {
          bp._1.addFileMeta(
            FileMeta(r.fingerprint, r.rowCount, r.dataFile))
        }
      ))
    // write updated metas down
    bAndP.foreach(bp => DataSourceMeta.write(
      new Path(bp._2.toString, OapFileFormat.OAP_META_FILE),
      configuration,
      bp._1.build(),
      deleteIfExits = true))
    Seq.empty
  }
}

/**
 * Drops an index
 */
case class DropIndexCommand(
    indexName: String,
    table: TableIdentifier,
    allowNotExists: Boolean,
    partitionSpec: Option[TablePartitionSpec]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val qe = sparkSession.sessionState.executePlan(UnresolvedRelation(table))
    qe.assertAnalyzed()
    val relation = qe.optimizedPlan

    val scheduler = sparkSession.sparkContext.schedulerBackend
    scheduler match {
      case _: CoarseGrainedSchedulerBackend =>
        OapRuntime.getOrCreate.oapRpcManager.send(CacheDrop(indexName))
      case _: LocalSchedulerBackend =>
        OapRuntime.getOrCreate.fiberCacheManager.removeIndexCache(indexName)
    }

    relation match {
      case LogicalRelation(HadoopFsRelation(fileCatalog, _, _, _, format, _), _, identifier)
          if format.isInstanceOf[OapFileFormat] || format.isInstanceOf[ParquetFileFormat] =>
        logInfo(s"Dropping index $indexName")
        val partitions = OapUtils.getPartitions(fileCatalog, partitionSpec)
        val targetDirs = partitions.filter(_.files.nonEmpty)
        if(targetDirs.isEmpty) {
          logWarning(s"""No data and Index in $partitions, DropIndexCommand do nothing.""")
          return Nil
        }
        val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(hadoopConfiguration)
        val dropIndexDirsCount = targetDirs.map(p => {
          val parent = p.files.head.getPath.getParent
          if (fs.exists(new Path(parent, OapFileFormat.OAP_META_FILE))) {
            val metaBuilder = new DataSourceMetaBuilder()
            val m = OapUtils.getMeta(hadoopConfiguration, parent)
            assert(m.nonEmpty)
            val oldMeta = m.get
            val existsIndexes = oldMeta.indexMetas
            val existsData = oldMeta.fileMetas
            if (existsIndexes.forall(_.name != indexName)) {
              logWarning(s"drop non-exists index $indexName")
              allowNotExists
            } else {
              if (existsData != null) {
                existsData.foreach(metaBuilder.addFileMeta)
              }
              if (existsIndexes != null) {
                existsIndexes.filter(_.name != indexName).foreach(metaBuilder.addIndexMeta)
              }
              metaBuilder.withNewDataReaderClassName(oldMeta.dataReaderClassName)
              DataSourceMeta.write(
                new Path(parent.toString, OapFileFormat.OAP_META_FILE),
                hadoopConfiguration,
                metaBuilder.withNewSchema(oldMeta.schema).build())
              fs.listStatus(parent, new PathFilter {
                override def accept(path: Path): Boolean = path.getName.endsWith(
                  "." + indexName + OapFileFormat.OAP_INDEX_EXTENSION)
              }).foreach(file => fs.delete(file.getPath, true))
              true
            }
          } else {
            false
          }
        }).count(_ == true)
        // No actually drop index action and allowNotExists is false, throw AnalysisException
        if (dropIndexDirsCount == 0 && !allowNotExists) {
          identifier match {
            case Some(catalogTable) =>
              throw new AnalysisException(s"""Index $indexName does not exist on $catalogTable""")
            case None =>
              val parent = targetDirs.head.files.head.getPath.getParent
              throw new AnalysisException(s"""Index $indexName does not exist on $parent""")
          }
        }
      case other => sys.error(s"We don't support index dropping for ${other.simpleString}")
    }
    Seq.empty
  }
}

/**
 * Refreshes an index for table
 */
case class RefreshIndexCommand(
    table: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec]) extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val qe = sparkSession.sessionState.executePlan(UnresolvedRelation(table))
    qe.assertAnalyzed()
    val optimized = qe.optimizedPlan

    val (fileCatalog, schema, readerClassName, relation) = optimized match {
      case LogicalRelation(
          HadoopFsRelation(f, _, s, _, _: OapFileFormat, _), _, _) =>
        (f, s, OapFileFormat.OAP_DATA_FILE_CLASSNAME, optimized)
      case LogicalRelation(
      _fsRelation @ HadoopFsRelation(f, _, s, _, format: ParquetFileFormat, _), attributes, id) =>
        // Use ReadOnlyParquetFileFormat instead of ParquetFileFormat because of
        // ReadOnlyParquetFileFormat.isSplitable always return false
        val fsRelation = _fsRelation.copy(
          fileFormat = new ReadOnlyParquetFileFormat(),
          options = _fsRelation.options)(_fsRelation.sparkSession)
        val logical = LogicalRelation(fsRelation, attributes, id)
        (f, s, OapFileFormat.PARQUET_DATA_FILE_CLASSNAME, logical)
      case other =>
        throw new OapException(s"We don't support index refreshing for ${other.simpleString}")
    }

    val configuration = sparkSession.sessionState.newHadoopConf()
    val partitions = OapUtils.getPartitions(fileCatalog, partitionSpec).filter(_.files.nonEmpty)
    // TODO currently we ignore empty partitions, so each partition may have different indexes,
    // this may impact index updating. It may also fail index existence check. Should put index
    // info at table level also.
    // aggregate all existing indices
    val indices = partitions.flatMap(p => {
      val parent = p.files.head.getPath.getParent
      // TODO get `fs` outside of map() to boost
      val fs = parent.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      val existOld = fs.exists(new Path(parent, OapFileFormat.OAP_META_FILE))
      if (existOld) {
        val m = OapUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, parent)
        assert(m.nonEmpty)
        val oldMeta = m.get
        oldMeta.indexMetas
      } else {
        Nil
      }
    }).groupBy(_.name).map(_._2.head)

    val bAndP = partitions.map(p => {
      val metaBuilder = new DataSourceMetaBuilder()
      val parent = p.files.head.getPath.getParent
      // TODO get `fs` outside of map() to boost
      val fs = parent.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      val existOld = fs.exists(new Path(parent, OapFileFormat.OAP_META_FILE))
      if (existOld) {
        val m = OapUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, parent)
        assert(m.nonEmpty)
        val oldMeta = m.get
        // add filemeta list already exist
        oldMeta.fileMetas.foreach(metaBuilder.addFileMeta)
        // TODO for now we only support data file adding before updating index
        metaBuilder.withNewSchema(oldMeta.schema)
      } else {
        metaBuilder.withNewSchema(schema)
      }
      indices.foreach(metaBuilder.addIndexMeta)
      // we cannot build meta for those without oap meta data
      metaBuilder.withNewDataReaderClassName(readerClassName)
      // when p.files is nonEmpty but no oap meta, it means the relation is in parquet(else
      // it is Oap empty partition, we won't create meta for them).
      // For Parquet, we only use Oap meta to track schema and reader class, as well as
      // `IndexMeta`s that must be empty at the moment, so `FileMeta`s are ok to leave empty.
      // p.files.foreach(f => builder.addFileMeta(FileMeta("", 0, f.getPath.toString)))
      (metaBuilder, parent)
    })

    val buildrst = indices.map(i => {
      var indexType: OapIndexType = BTreeIndexType

      val indexColumns = i.indexType match {
        case BTreeIndex(entries) =>
          entries.map(e => IndexColumn(schema(e.ordinal).name, e.dir == Ascending))
        case BitMapIndex(entries) =>
          indexType = BitMapIndexType
          entries.map(e => IndexColumn(schema(e).name))
        case it => sys.error(s"Not implemented index type $it")
      }

      val projectList = indexColumns.map { indexColumn =>
        relation.output.find(p => p.name == indexColumn.columnName).get.withMetadata(
          new MetadataBuilder().putBoolean("isAscending", indexColumn.isAscending).build())
      }

      var ds = Dataset.ofRows(sparkSession, Project(projectList, relation))
      partitionSpec.getOrElse(Map.empty).foreach { case (k, v) =>
        ds = ds.filter(s"$k='$v'")
      }

      val outPutPath = OapUtils.getOutPutPath(fileCatalog)
      assert(outPutPath != null, "Expected exactly one path to be specified, but no value")

      val qualifiedOutputPath = {
        val fs = outPutPath.getFileSystem(configuration)
        outPutPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      }

      val committer = FileCommitProtocol.instantiate(
        sparkSession.sessionState.conf.fileCommitProtocolClass,
        jobId = java.util.UUID.randomUUID().toString,
        outputPath = outPutPath.toUri.getPath,
        isAppend = false)

      val options = Map(
        "indexName" -> i.name,
        "indexTime" -> i.time,
        "isAppend" -> "true",
        "indexType" -> indexType.toString
      )

      FileFormatWriter.write(
        sparkSession = sparkSession,
        queryExecution = ds.queryExecution,
        fileFormat = new OapIndexFileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(
          qualifiedOutputPath.toUri.getPath, Map.empty),
        hadoopConf = configuration,
        partitionColumns = Seq.empty,
        bucketSpec = Option.empty,
        refreshFunction = _ => Unit,
        options = options).asInstanceOf[Seq[Seq[IndexBuildResult]]]
    })
    if (buildrst.nonEmpty) {
      val retMap = buildrst.head.flatten.groupBy(_.parent)

      // there some cases oap meta files have already been updated
      // e.g. when inserting data in oap files the meta has already updated
      // so, we should ignore these cases
      // And files modifications for parquet should refresh oap meta in this way
      val filteredBAndP = bAndP.filter(x => retMap.contains(x._2.toString))
      filteredBAndP.foreach(bp =>
        retMap.getOrElse(bp._2.toString, Nil).foreach(r => {
          if (!bp._1.containsFileMeta(r.dataFile)) {
            bp._1.addFileMeta(FileMeta(r.fingerprint, r.rowCount, r.dataFile))
          }
        }
      ))

      // write updated metas down
      filteredBAndP.foreach(bp => DataSourceMeta.write(
        new Path(bp._2.toString, OapFileFormat.OAP_META_FILE),
        sparkSession.sparkContext.hadoopConfiguration,
        bp._1.build(),
        deleteIfExits = true))

      fileCatalog.refresh()
    }

    Seq.empty
  }
}

/**
 * List indices for table
 */
case class OapShowIndexCommand(table: TableIdentifier, relationName: String)
  extends RunnableCommand with Logging {

  override val output: Seq[Attribute] = {
    AttributeReference("table", StringType, nullable = true)() ::
      AttributeReference("key_name", StringType, nullable = false)() ::
      AttributeReference("seq_in_index", IntegerType, nullable = false)() ::
      AttributeReference("column_name", StringType, nullable = false)() ::
      AttributeReference("collation", StringType, nullable = true)() ::
      AttributeReference("index_type", StringType, nullable = false)() ::
      AttributeReference("enabled", BooleanType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val qe = sparkSession.sessionState.executePlan(UnresolvedRelation(table))
    qe.assertAnalyzed()
    val relation = qe.optimizedPlan

    val (fileCatalog, schema) = relation match {
      case LogicalRelation(HadoopFsRelation(f, _, s, _, _, _), _, id) =>
        (f, s)
      case other =>
        throw new OapException(s"We don't support index listing for ${other.simpleString}")
    }

    val partitions = OapUtils.getPartitions(fileCatalog).filter(_.files.nonEmpty)
    // TODO currently we ignore empty partitions, so each partition may have different indexes,
    // this may impact index updating. It may also fail index existence check. Should put index
    // info at table level also.
    // aggregate all existing indices
    val indices = partitions.flatMap(p => {
      val parent = p.files.head.getPath.getParent
      // TODO get `fs` outside of map() to boost
      val fs = parent.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      val existOld = fs.exists(new Path(parent, OapFileFormat.OAP_META_FILE))
      if (existOld) {
        val m = OapUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, parent)
        assert(m.nonEmpty)
        val oldMeta = m.get
        oldMeta.indexMetas
      } else {
        Nil
      }
    }).groupBy(_.name).map(_._2.head)
    val disableIndexList =
      sparkSession.conf.get(OapConf.OAP_INDEX_DISABLE_LIST).split(",").map(_.trim)
    indices.toSeq.flatMap(i => i.indexType match {
      case BTreeIndex(entries) =>
        val enabled = !disableIndexList.contains(i.name)
        entries.zipWithIndex.map(ei => {
          val dir = if (ei._1.dir == Ascending) "A" else "D"
          Row(relationName, i.name, ei._2, schema(ei._1.ordinal).name, dir, "BTREE", enabled)
        })
      case BitMapIndex(entries) =>
        val enabled = !disableIndexList.contains(i.name)
        entries.zipWithIndex.map(ei =>
          Row(relationName, i.name, ei._2, schema(ei._1).name, "A", "BITMAP", enabled))
      case t => sys.error(s"not support index type $t for index ${i.name}")
    })
  }
}

/**
 * Check integrity of data and indices for specified table
 * Invoked by `CHECK OINDEX ON table`
 * Currently it has the following features:
 * 1. check existence of oap meta file
 * 2. check integrity of each partition directory of table for both data files
 *    and index files according to meta
 * @param table represents the table identifier
 * @param tableName table name of the specified table
 */
case class OapCheckIndexCommand(
    table: TableIdentifier,
    tableName: String,
    partitionSpec: Option[TablePartitionSpec]) extends RunnableCommand with Logging {
  override val output: Seq[Attribute] =
    AttributeReference("Analysis Result", StringType, nullable = false)() :: Nil

  private def checkOapMetaFile(
      fs: FileSystem,
      partitionDirs: Seq[Path]): (Seq[Path], Seq[Path]) = {
    require(null ne fs, "file system should not be null!")

    partitionDirs.partition(partitionDir =>
      fs.exists(new Path(partitionDir, OapFileFormat.OAP_META_FILE)))
  }

  private def processPartitionsWithNoMeta(partitionDirs: Seq[Path]): Seq[Row] = {
    partitionDirs.map(partitionPath =>
      Row(s"Meta file not found in partition: ${partitionPath.toUri.getPath}"))
  }

  private def checkEachPartition(
      sparkSession: SparkSession,
      fs: FileSystem,
      dataSchema: StructType,
      partitionDir: Path): Seq[Row] = {
    require(null ne fs, "file system should not be null!")
    val m = OapUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, partitionDir)
    assert(m.nonEmpty)
    val fileMetas = m.get.fileMetas
    val indexMetas = m.get.indexMetas
    checkDataFileInEachPartition(fs, dataSchema, fileMetas, partitionDir) ++
      checkIndexInEachPartition(fs, dataSchema, fileMetas, indexMetas, partitionDir)
  }

  private def checkDataFileInEachPartition(
      fs: FileSystem,
      dataSchema: StructType,
      fileMetas: Seq[FileMeta],
      partitionPath: Path): Seq[Row] = {
    require(null ne fs, "file system should not be null!")
    fileMetas.filterNot(file_meta => fs.exists(new Path(partitionPath, file_meta.dataFileName)))
      .map(file_meta =>
        Row(s"Data file: ${partitionPath.toUri.getPath}/${file_meta.dataFileName} not found!"))
  }

  private def checkIndexInEachPartition(
      fs: FileSystem,
      dataSchema: StructType,
      fileMetas: Seq[FileMeta],
      indexMetas: Seq[IndexMeta],
      partitionPath: Path): Seq[Row] = {
    require(null ne fs, "file system should not be null!")
    indexMetas.flatMap(index_meta => {
      val (indexType, indexColumns) = index_meta.indexType match {
        case BTreeIndex(entries) =>
          ("BTree", entries.map(e => dataSchema(e.ordinal).name).mkString(","))
        case BitMapIndex(entries) =>
          ("Bitmap", entries.map(dataSchema(_).name).mkString(","))
        case HashIndex(entries) =>
          ("Bitmap", entries.map(dataSchema(_).name).mkString(","))
        case other => throw new OapException(s"We don't support this type of index: $other")
      }
      val dataFilesWithoutIndices = fileMetas.filter { file_meta =>
        val indexFile =
          IndexUtils.indexFileFromDataFile(new Path(partitionPath, file_meta.dataFileName),
            index_meta.name, index_meta.time)
        !fs.exists(indexFile)
      }
      dataFilesWithoutIndices.map(file_meta =>
        Row(
          s"""Missing index:${index_meta.name},
            |indexColumn(s): $indexColumns, indexType: $indexType
            |for Data File: ${partitionPath.toUri.getPath}/${file_meta.dataFileName}
            |of table: $tableName""".stripMargin))
    })
  }

  private def analyzeIndexBetweenPartitions(
      sparkSession: SparkSession,
      fs: FileSystem,
      partitionDirs: Seq[Path]): Unit = {
    require(null ne fs, "file system should not be null!")
    val indicesMap = new mutable.HashMap[String, (IndexType, Seq[Path])]()
    val ambiguousIndices = new mutable.HashSet[String]()
    partitionDirs.foreach { partitionDir =>
      val m = OapUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, partitionDir)
      assert(m.nonEmpty)
      m.get.indexMetas.foreach { index_meta =>
        val (idxType, idxPaths) =
          indicesMap.getOrElse(index_meta.name, (index_meta.indexType, Seq.empty[Path]))

        if (!ambiguousIndices.contains(index_meta.name) &&
          indicesMap.contains(index_meta.name) && index_meta.indexType != idxType) {
          ambiguousIndices.add(index_meta.name)
        }

        indicesMap.put(index_meta.name, (idxType, idxPaths :+ partitionDir))
      }
    }

    if (ambiguousIndices.nonEmpty) {
      val sb = new StringBuilder
      ambiguousIndices.foreach(indexName => {
        sb.append("Ambiguous Index(different indices have the same name):\n")
        sb.append("index name:")
        sb.append(indexName)
        sb.append("\nin partition:\n")
        indicesMap(indexName)._2.map(_.toUri.getPath).addString(sb, "\n")
        sb.append("\n")
      })
      throw new AnalysisException(s"\n${sb.toString()}")
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val qe = sparkSession.sessionState.executePlan(UnresolvedRelation(table))
    qe.assertAnalyzed()
    val relation = qe.optimizedPlan

    val (fileCatalog, dataSchema) = relation match {
      case LogicalRelation(HadoopFsRelation(f, _, s, _, _, _), _, id) =>
        (f, s)
      case other =>
        throw new OapException(s"We don't support index checking for ${other.simpleString}")
    }

    val rootPaths = fileCatalog.rootPaths
    val fs = if (rootPaths.nonEmpty) {
      rootPaths.head.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    } else {
      null
    }

    if (rootPaths.isEmpty || (null eq fs)) {
      Seq.empty
    } else {
      val partitionDirs =
        OapUtils.getPartitionPaths(rootPaths, fs, fileCatalog.partitionSchema, partitionSpec)

      val (partitionWithMeta, partitionWithNoMeta) = checkOapMetaFile(fs, partitionDirs)
      analyzeIndexBetweenPartitions(sparkSession, fs, partitionWithMeta)
      processPartitionsWithNoMeta(partitionWithNoMeta) ++
        partitionWithMeta.flatMap(checkEachPartition(sparkSession, fs, dataSchema, _))
    }

  }
}

/**
 * Disable specific index
 */
case class OapDisableIndexCommand(indexName: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val beforeList = sparkSession.conf.get(OapConf.OAP_INDEX_DISABLE_LIST.key)
      .split(",").map(_.trim).filterNot(_ == "") // The filter deals with empty String
    val afterList = if (beforeList.contains(indexName)) {
      beforeList
    } else {
      beforeList :+ indexName
    }
    sparkSession.conf.set(OapConf.OAP_INDEX_DISABLE_LIST.key, afterList.mkString(", "))
    Seq.empty
  }
}

/**
 * Enable disabled index
 */
case class OapEnableIndexCommand(indexName: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val disableList = sparkSession.conf.get(OapConf.OAP_INDEX_DISABLE_LIST).split(",").map(_.trim)
    val refreshedDisableList = disableList.filterNot(_ == indexName)
    if (disableList.length == refreshedDisableList.length) {
      logWarning(s"Index $indexName hasn't been disabled")
    }
    sparkSession.conf.set(OapConf.OAP_INDEX_DISABLE_LIST.key, refreshedDisableList.mkString(", "))
    Seq.empty
  }
}
