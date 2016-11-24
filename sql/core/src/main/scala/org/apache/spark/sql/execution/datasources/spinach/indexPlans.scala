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

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Descending}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, SpinachException}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.spinach.utils.SpinachUtils

/**
  * Creates an index for table on indexColumns
  */
case class CreateIndex(indexName: String,
                       tableLP: LogicalPlan,
                       tableName: TableIdentifier,
                       indexColumns: Array[IndexColumn],
                       allowExists: Boolean) extends RunnableCommand with Logging {
  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def analyzed: Boolean =
    if (tableLP == null) false else tableLP.analyzed

  override lazy val resolved: Boolean =
    if (tableLP == null) false else tableLP.resolved

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    assert(catalog.tableExists(tableName), s"$tableName not exists")

    val (fileCatalog, s, readerClassName) = catalog.lookupRelation(tableName) match {
      case SubqueryAlias(_, LogicalRelation(
      HadoopFsRelation(_, fileCatalog, _, s, _, _: SpinachFileFormat, _), _, _)) =>
        (fileCatalog, s, SpinachFileFormat.SPINACH_DATA_FILE_CLASSNAME)
      case SubqueryAlias(_, LogicalRelation(
      HadoopFsRelation(_, fileCatalog, _, s, _, _: ParquetFileFormat, _), _, _)) =>
        (fileCatalog, s, SpinachFileFormat.PARQUET_DATA_FILE_CLASSNAME)
      case other =>
        throw new SpinachException(s"We don't support index building for ${other.simpleString}")
    }

    logInfo(s"Creating index $indexName")
    val partitions = SpinachUtils.getPartitions(fileCatalog)
    // TODO currently we ignore empty partitions, so each partition may have different indexes,
    // this may impact index updating. It may also fail index existence check. Should put index
    // info at table level also.
    val bAndP = partitions.filter(_.files.nonEmpty).map(p => {
      val metaBuilder = new DataSourceMetaBuilder()
      val parent = p.files.head.getPath.getParent
      // TODO get `fs` outside of map() to boost
      val fs = parent.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      val existOld = fs.exists(new Path(parent, SpinachFileFormat.SPINACH_META_FILE))
      if (existOld) {
        val m = SpinachUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, parent)
        assert(m.nonEmpty)
        val oldMeta = m.get
        val existsIndexes = oldMeta.indexMetas
        val existsData = oldMeta.fileMetas
        if (existsIndexes.exists(_.name == indexName)) {
          if (!allowExists) {
            throw new AnalysisException(s"""Index $indexName exists on table $tableName""")
          } else {
            logWarning(s"Dup index name $indexName")
          }
        }
        if (existsData != null) existsData.foreach(metaBuilder.addFileMeta)
        if (existsIndexes != null) {
          existsIndexes.filter(_.name != indexName).foreach(metaBuilder.addIndexMeta)
        }
        metaBuilder.withNewSchema(oldMeta.schema)
      } else {
        metaBuilder.withNewSchema(s)
      }
      val entries = indexColumns.map(c => {
        val dir = if (c.isAscending) Ascending else Descending
        BTreeIndexEntry(s.map(_.name).toIndexedSeq.indexOf(c.columnName), dir)
      })
      metaBuilder.addIndexMeta(new IndexMeta(indexName, BTreeIndex(entries)))
      // we cannot build meta for those without spinach meta data
      metaBuilder.withNewDataReaderClassName(readerClassName)
      // when p.files is nonEmpty but no spinach meta, it means the relation is in parquet(else
      // it is Spinach empty partition, we won't create meta for them).
      // For Parquet, we only use Spinach meta to track schema and reader class, as well as
      // `IndexMeta`s that must be empty at the moment, so `FileMeta`s are ok to leave empty.
      // p.files.foreach(f => builder.addFileMeta(FileMeta("", 0, f.getPath.toString)))
      (metaBuilder, parent, existOld)
    })
    val ret = SpinachIndexBuild(
      sparkSession, indexName, indexColumns, s, bAndP.map(_._2), readerClassName).execute()
    val retMap = ret.groupBy(_.parent)
    bAndP.foreach(bp =>
      retMap.getOrElse(bp._2.toString, Nil).foreach(r =>
        if (!bp._3) bp._1.addFileMeta(FileMeta(r.fingerprint, r.rowCount, r.dataFile)))
    )
    // write updated metas down
    bAndP.foreach(bp => DataSourceMeta.write(
      new Path(bp._2.toString, SpinachFileFormat.SPINACH_META_FILE),
      sparkSession.sparkContext.hadoopConfiguration,
      bp._1.build(),
      deleteIfExits = true))
    Seq.empty
  }
}

/**
 * Drops an index
 */
case class DropIndex(
    indexName: String,
    tableIdentifier: TableIdentifier,
    allowNotExists: Boolean) extends RunnableCommand {

  override def children: Seq[LogicalPlan] = Seq.empty

  override val output: Seq[Attribute] = Seq.empty

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    catalog.lookupRelation(tableIdentifier) match {
      case SubqueryAlias(_, LogicalRelation(
          HadoopFsRelation(_, fileCatalog, _, _, _, format, _), _, _))
          if format.isInstanceOf[SpinachFileFormat] || format.isInstanceOf[ParquetFileFormat] =>
        logInfo(s"Dropping index $indexName")
        val partitions = SpinachUtils.getPartitions(fileCatalog)
        partitions.filter(_.files.nonEmpty).foreach(p => {
          val parent = p.files.head.getPath.getParent
          // TODO get `fs` outside of foreach() to boost
          val fs = parent.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
          if (fs.exists(new Path(parent, SpinachFileFormat.SPINACH_META_FILE))) {
            val metaBuilder = new DataSourceMetaBuilder()
            val m = SpinachUtils.getMeta(sparkSession.sparkContext.hadoopConfiguration, parent)
            assert(m.nonEmpty)
            val oldMeta = m.get
            val existsIndexes = oldMeta.indexMetas
            val existsData = oldMeta.fileMetas
            if (!existsIndexes.exists(_.name == indexName)) {
              if (!allowNotExists) {
                throw new AnalysisException(
                  s"""Index $indexName not exists on table $tableIdentifier""")
              } else {
                logWarning(s"drop non-exists index $indexName")
              }
            }
            if (existsData != null) existsData.foreach(metaBuilder.addFileMeta)
            if (existsIndexes != null) {
              existsIndexes.filter(_.name != indexName).foreach(metaBuilder.addIndexMeta)
            }
            metaBuilder.withNewDataReaderClassName(oldMeta.dataReaderClassName)
            DataSourceMeta.write(
              new Path(parent.toString, SpinachFileFormat.SPINACH_META_FILE),
              sparkSession.sparkContext.hadoopConfiguration,
              metaBuilder.withNewSchema(oldMeta.schema).build(),
              deleteIfExits = true)
            val allFile = fs.listFiles(parent, false)
            val filePaths = new Iterator[Path] {
              override def hasNext: Boolean = allFile.hasNext
              override def next(): Path = allFile.next().getPath
            }.toSeq
            filePaths.filter(_.toString.endsWith(
              "." + indexName + SpinachFileFormat.SPINACH_INDEX_EXTENSION)).foreach(
              fs.delete(_, true))
          }
        })
      case _ => sys.error("We don't support index dropping for ${other.simpleString}")
    }
    Seq.empty
  }
}
