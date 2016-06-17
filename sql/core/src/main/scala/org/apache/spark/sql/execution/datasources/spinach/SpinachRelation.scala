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

import java.io.ByteArrayOutputStream
import java.util.Comparator

import scala.collection.JavaConverters._

import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce._

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLConf, SQLContext}
import org.apache.spark.util.SerializableConfiguration

class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "spn"

  override def createRelation(
     sqlContext: SQLContext,
     paths: Array[String],
     dataSchema: Option[StructType],
     partitionColumns: Option[StructType],
     parameters: Map[String, String]): HadoopFsRelation = {
    new SpinachRelation(paths, dataSchema, None, partitionColumns, parameters)(sqlContext)
  }
}

private[spinach] class SpinachRelation(
    override val paths: Array[String],
    maybeDataSchema: Option[StructType],
    maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    @transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec, parameters)
  with Logging {

  private[spinach] def this(
                         paths: Array[String],
                         maybeDataSchema: Option[StructType],
                         maybePartitionSpec: Option[PartitionSpec],
                         parameters: Map[String, String])(
                         sqlContext: SQLContext) = {
    this(
      paths,
      maybeDataSchema,
      maybePartitionSpec,
      maybePartitionSpec.map(_.partitionColumns),
      parameters)(sqlContext)
  }

  // get the meta & data file path.
  private def _metaPaths: Array[FileStatus] =
    cachedLeafStatuses().filter { status =>
      status.getPath.getName.endsWith(SpinachFileFormat.SPINACH_META_FILE)
    }.toArray

  private def meta: Option[DataSourceMeta] = {
    if (_metaPaths.isEmpty) {
      None
    } else {
      // TODO verify all of the schema from the meta data
      Some(DataSourceMeta.initialize(
        _metaPaths(0).getPath,
        sqlContext.sparkContext.hadoopConfiguration))
    }
  }

  private def createIndexContext: IndexContext = {
    meta.map(new IndexContext(_)).getOrElse(DummyIndexContext)
  }

  override val dataSchema: StructType = maybeDataSchema.getOrElse(
    meta.map(_.schema)
      .getOrElse(
        throw new IllegalStateException("Cannot get the meta info from file spinach.meta")))

  override def needConversion: Boolean = false

  override def equals(other: Any): Boolean = other match {
    case that: SpinachRelation =>
      paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema &&
        partitionColumns == that.partitionColumns
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      paths.toSet,
      dataSchema,
      schema,
      partitionColumns)
  }

  override def buildInternalScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[InternalRow] = {
    // TODO this probably used by column pruning
    // val output = StructType(requiredColumns.map(dataSchema(_))).toAttributes
    // get the data path from the given paths
    // TODO get the index file from the given paths

    meta match {
      case Some(mt) =>
        val indexContext = createIndexContext
        BPlusTreeSearch.build(filters, indexContext)
        SpinachTableScan(mt, this, indexContext.getScannerBuilder.map(_.build),
          requiredColumns, inputPaths, broadcastedConf).execute()
      case None =>
        sqlContext.sparkContext.emptyRDD[InternalRow]
    }
  }

  // currently we don't support any filtering.
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    BPlusTreeSearch.build(filters, createIndexContext)
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new SpinachOutputWriterFactory
  }

  override def createWriteContainer(df: DataFrame, isAppend: Boolean, job: Job)
  : BaseWriterContainer = {
//    // used by initialize the outputCommitter.
//    job.setOutputFormatClass(classOf[SpinachFileOutputFormat])

    if (partitionColumns.isEmpty) {
      new NonDynamicPartitionWriteContainer(this, job, isAppend, df.schema)
    } else {
      val output = df.queryExecution.executedPlan.output
      val partitionNames = partitionColumns.fieldNames
      val (partitionOutput, dataOutput) =
        output.partition(a => partitionNames.contains(a.name))

      new DynamicPartitionWriterContainer(
        this,
        job,
        partitionOutput,
        dataOutput,
        output,
        PartitioningUtils.DEFAULT_PARTITION_NAME,
        sqlContext.conf.getConf(SQLConf.PARTITION_MAX_FILES),
        isAppend,
        df.schema)
    }
  }

  def createIndex(
      indexName: String, indexColumns: Array[IndexColumn], allowExists: Boolean): Unit = {
    logInfo(s"Creating index $indexName")
    meta match {
      case Some(oldMeta) =>
        val existsIndexes = oldMeta.indexMetas
        val exist = existsIndexes.exists(_.name == indexName)
        if (!allowExists) assert(!exist)
        if (exist) {
          logWarning(s"dup index name $indexName")
          return
        }
        val metaBuilder = DataSourceMeta.newBuilder()
        oldMeta.fileMetas.foreach(metaBuilder.addFileMeta)
        existsIndexes.foreach(metaBuilder.addIndexMeta)
        val entries = indexColumns.map(c => {
          val dir = if (c.isAscending) Ascending else Descending
          BTreeIndexEntry(schema.map(_.name).toIndexedSeq.indexOf(c.columnName), dir)
        })
        metaBuilder.addIndexMeta(new IndexMeta(indexName, BTreeIndex(entries)))

        // TODO _metaPaths can be empty while in an empty spinach data source folder
        DataSourceMeta.write(
          _metaPaths(0).getPath,
          sqlContext.sparkContext.hadoopConfiguration,
          metaBuilder.withNewSchema(oldMeta.schema).build(),
          deleteIfExits = true)
        SpinachIndexBuild(sqlContext, indexName, indexColumns, schema, paths).execute()
      case None =>
        sys.error("meta cannot be empty during the index building")
    }
  }

  def dropIndex(indexName: String, allowNotExists: Boolean): Unit = {
    logInfo(s"Dropping index $indexName")
    assert(meta.nonEmpty)
    val oldMeta = meta.get
    val existsIndexes = oldMeta.indexMetas
    val exist = existsIndexes.exists(_.name == indexName)
    if (!allowNotExists) assert(exist, "IndexMeta not found in SpinachMeta")
    if (!exist) {
      logWarning(s"drop non-exists index $indexName")
      return
    }
    val metaBuilder = DataSourceMeta.newBuilder()
    oldMeta.fileMetas.foreach(metaBuilder.addFileMeta)
    existsIndexes.filter(_.name != indexName).foreach(metaBuilder.addIndexMeta)

    val path = new Path(paths(0))
    val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val allFile = fs.listFiles(path, true)
    val filePaths = new Iterator[Path] {
      override def hasNext: Boolean = allFile.hasNext
      override def next(): Path = allFile.next().getPath
    }.toSeq
    filePaths.filter(_.toString.endsWith(
      "." + indexName + SpinachFileFormat.SPINACH_INDEX_EXTENSION)).foreach(fs.delete(_, true))

    DataSourceMeta.write(
      _metaPaths(0).getPath,
      sqlContext.sparkContext.hadoopConfiguration,
      metaBuilder.withNewSchema(oldMeta.schema).build(),
      deleteIfExits = true)
  }
}

private[spinach] class SpinachOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext) extends OutputWriter {
  private val writer = new FileOutputFormat[NullWritable, InternalRow] {
    override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
      new Path(path, getFileName(extension))
    }

    override def getRecordWriter(context: TaskAttemptContext)
    : RecordWriter[NullWritable, InternalRow] = {
      val conf: Configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
      val isCompressed: Boolean = FileOutputFormat.getCompressOutput(context)

      val file: Path = getDefaultWorkFile(context, SpinachFileFormat.SPINACH_DATA_EXTENSION)
      val fs: FileSystem = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      new SpinachDataWriter2(isCompressed, fileOut, dataSchema)
    }
  }.getRecordWriter(context)

  override def write(row: Row): Unit = throw new NotImplementedError("write(row: Row)")
  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    writer.write(NullWritable.get(), row)
  }
  override def close(): Unit = writer.close(context)

  def getFileName(extension: String): String = {
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    // this is the way how we pass down the uuid
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
    val split = taskAttemptId.getTaskID.getId
    f"part-r-$split%05d-${uniqueWriteJobId}$extension"
  }

  def getFileName(): String = getFileName(SpinachFileFormat.SPINACH_DATA_EXTENSION)
}

private[spinach] class SpinachOutputWriterFactory extends OutputWriterFactory {
  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    // TODO possible to do some setup on executor side initialization
    new SpinachOutputWriter(path, dataSchema, context)
  }
}

private[spinach] case class SpinachTableScan(
    meta: DataSourceMeta,
    @transient relation: SpinachRelation,
    scanner: Option[RangeScanner],
    requiredColumns: Array[String],
    @transient inputPaths: Array[FileStatus],
    bc: Broadcast[SerializableConfiguration])
  extends Logging {
  @transient private val sqlContext = relation.sqlContext

  // TODO serialize / deserialize the schema via SpinachMeta instead
  def execute(): RDD[InternalRow] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    // TODO Tries to push down filters if spinach filter push-down is enabled
    if (inputPaths.isEmpty) {
      // the input path probably be pruned, return an empty RDD.
      return sqlContext.sparkContext.emptyRDD[InternalRow]
    }
    // TODO write our own RDD, so we can pass down the info via closure instead
    FileInputFormat.setInputPaths(job, inputPaths.map(_.getPath): _*)
    conf.set(SpinachFileFormat.SPINACH_META_SCHEMA, meta.schema.json)

    if (scanner.nonEmpty) {
      // serialize the scanner
      SpinachFileFormat.serializeFilterScanner(conf, scanner.get)
    }
    // serialize the required column
    SpinachFileFormat.setRequiredColumnIds(conf, meta.schema, requiredColumns)

    sqlContext.sparkContext.newAPIHadoopRDD(
      conf, classOf[SpinachFileInputFormat], classOf[NullWritable], classOf[InternalRow]).map(_._2)
  }
}

private[spinach] case class SpinachIndexBuild(
    sqlContext: SQLContext,
    indexName: String,
    indexColumns: Array[IndexColumn],
    schema: StructType,
    paths: Array[String]) extends Logging {
  @transient private lazy val ids =
    indexColumns.map(c => schema.map(_.name).toIndexedSeq.indexOf(c.columnName))
  @transient private lazy val keySchema = StructType(ids.map(schema.toIndexedSeq(_)))
  def execute(): RDD[InternalRow] = {
    if (paths.isEmpty) {
      // the input path probably be pruned, do nothing
    } else {
      // TODO use internal scan
      val path = paths(0)
      @transient val p = new Path(path)
      @transient val fs = p.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      @transient val fileIter = fs.listFiles(p, true)
      @transient val dataPaths = new Iterator[Path] {
        override def hasNext: Boolean = fileIter.hasNext
        override def next(): Path = fileIter.next().getPath
      }.toSeq
      val data = dataPaths.map(_.toString).filter(
        _.endsWith(SpinachFileFormat.SPINACH_DATA_EXTENSION))
      assert(!ids.exists(id => id < 0), "Index column not exists in schema.")
      @transient lazy val ordering = buildOrdering(ids, keySchema)
      val serializableConfiguration =
        new SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration)
      val confBroadcast = sqlContext.sparkContext.broadcast(serializableConfiguration)
      val num = dataPaths.length
      sqlContext.sparkContext.parallelize(data, num).map(dataString => {
      // data.foreach(dataString => {
        val d = new Path(dataString)
        // scan every data file
        val reader = new SpinachDataReader2(d, schema, None, ids)
        val hadoopConf = confBroadcast.value.value
        // TODO better initialize it elegantly
        val attemptContext: TaskAttemptContext = new TaskAttemptContextImpl(
          hadoopConf, new TaskAttemptID(new TaskID(new JobID(), true, 0), 0))
        reader.initialize(null, attemptContext)
        // TODO maybe use Long as RowId?
        val hashMap = new java.util.HashMap[InternalRow, java.util.ArrayList[Int]]()
        var cnt = 0
        while (reader.nextKeyValue()) {
          val v = reader.getCurrentValue.copy()
          if (!hashMap.containsKey(v)) {
            val list = new java.util.ArrayList[Int]()
            list.add(cnt)
            hashMap.put(v, list)
          } else {
            hashMap.get(v).add(cnt)
          }
          cnt = cnt + 1
        }
        reader.close()
        val partitionUniqueSize = hashMap.size()
        val uniqueKeys = hashMap.keySet().toArray(new Array[InternalRow](partitionUniqueSize))
        assert(uniqueKeys.size == partitionUniqueSize)
        lazy val comparator: Comparator[InternalRow] = new Comparator[InternalRow]() {
          override def compare(o1: InternalRow, o2: InternalRow): Int = {
            if (o1 == null && o2 == null) {
              0
            } else if (o1 == null) {
              -1
            } else if (o2 == null) {
              1
            } else {
              ordering.compare(o1, o2)
            }
          }
        }
        // sort keys
        java.util.Arrays.sort(uniqueKeys, comparator)
        // build index file
        val dataFilePathString = d.toString
        val pos = dataFilePathString.lastIndexOf(SpinachFileFormat.SPINACH_DATA_EXTENSION)
        val indexFile = new Path(dataFilePathString.substring(
          0, pos) + "." + indexName + SpinachFileFormat.SPINACH_INDEX_EXTENSION)
        val fs = indexFile.getFileSystem(hadoopConf)
        val fileOut = fs.create(indexFile, false)
        var i = 0
        var fileOffset = 0
        val offsetMap = new java.util.HashMap[InternalRow, Int]()
        // write data segment.
        while (i < partitionUniqueSize) {
          offsetMap.put(uniqueKeys(i), fileOffset)
          val rowIds = hashMap.get(uniqueKeys(i))
          // row count for same key
          IndexUtils.writeInt(fileOut, rowIds.size())
          fileOffset = fileOffset + 4
          var idIter = 0
          while (idIter < rowIds.size()) {
            IndexUtils.writeInt(fileOut, rowIds.get(idIter))
            fileOffset = fileOffset + 4
            idIter = idIter + 1
          }
          i = i + 1
        }
        val dataEnd = fileOffset
        // write index segment.
        val treeShape = BTreeUtils.generate2(partitionUniqueSize)
        val uniqueKeysList = new java.util.LinkedList[InternalRow]()
        import scala.collection.JavaConverters._
        uniqueKeysList.addAll(uniqueKeys.toSeq.asJava)
        writeTreeToOut(treeShape, fileOut, offsetMap, fileOffset, uniqueKeysList, keySchema, 0, -1)
        assert(uniqueKeysList.size == 1)
        IndexUtils.writeInt(fileOut, dataEnd)
        IndexUtils.writeInt(fileOut, offsetMap.get(uniqueKeysList.getFirst))
        fileOut.close()
        indexFile.toString
      }).collect()
    }
    sqlContext.sparkContext.emptyRDD[InternalRow]
  }

  private def buildOrdering(
      requiredIds: Array[Int], keySchema: StructType): Ordering[InternalRow] = {
    val order = requiredIds.toSeq.map(id => SortOrder(
      BoundReference(id, keySchema(id).dataType, nullable = true),
      if (indexColumns(requiredIds.indexOf(id)).isAscending) Ascending else Descending))
    GenerateOrdering.generate(order, keySchema.toAttributes)
  }

  private def writeTreeToOut(
      tree: BTreeNode,
      out: FSDataOutputStream,
      map: java.util.HashMap[InternalRow, Int],
      fileOffset: Int,
      keysList: java.util.LinkedList[InternalRow],
      keySchema: StructType,
      listOffsetFromEnd: Int,
      nextOffset: Int): (Int, Int) = {
    var subOffset = 0
    if (tree.children.nonEmpty) {
      // this is a non-leaf node
      // Need to write down all subtrees
      val childrenCount = tree.children.size
      assert(childrenCount == tree.root)
      var iter = childrenCount
      // write down all subtrees reversely
      var lastStart = nextOffset
      while (iter > 0) {
        iter -= 1
        val subTree = tree.children(iter)
        val subListOffsetFromEnd = listOffsetFromEnd + childrenCount - 1 - iter
        val (writeOffset, oneLevelStart) = writeTreeToOut(
          subTree, out, map, fileOffset + subOffset,
          keysList, keySchema, subListOffsetFromEnd, lastStart)
        lastStart = oneLevelStart
        subOffset += writeOffset
      }
    }
    (subOffset + writeIndexNode2(
      tree, out, map, keysList, listOffsetFromEnd, nextOffset, subOffset + fileOffset), subOffset)
  }

  @transient private lazy val converter = UnsafeProjection.create(keySchema)

  /**
   * write file correspond to UnsafeIndexNode22
   */
  private def writeIndexNode2(
      tree: BTreeNode,
      out: FSDataOutputStream,
      map: java.util.HashMap[InternalRow, Int],
      keysList: java.util.LinkedList[InternalRow],
      listOffsetFromEnd: Int,
      nextOffset: Int,
      updateOffset: Int): Int = {
    var subOffset = 0
    // write road sign count on every node first
    IndexUtils.writeInt(out, tree.root)
    subOffset = subOffset + 4
    IndexUtils.writeInt(out, nextOffset)
    subOffset = subOffset + 4
    // For all IndexNode, write down all road sign, each pointing to specific data segment
    val start = keysList.size - listOffsetFromEnd - tree.root
    val end = keysList.size - listOffsetFromEnd
    val writeList = keysList.subList(start, end).asScala
    val keyBuf = new ByteArrayOutputStream()
    // offset0 pointer0, offset1 pointer1, ..., offset(n-1) pointer(n-1),
    // len0 key0, len1 key1, ..., len(n-1) key(n-1)
    val baseOffset = updateOffset + subOffset + tree.root * 8
    var i = 0
    while (i < tree.root) {
      val writeKey = writeList(i)
      IndexUtils.writeInt(out, baseOffset + keyBuf.size)
      // assert(map.containsKey(writeList(i)))
      IndexUtils.writeInt(out, map.get(writeKey))
      subOffset += 8
      val writeRow = converter.apply(writeKey)
      IndexUtils.writeInt(keyBuf, writeRow.getSizeInBytes)
      writeRow.writeToStream(keyBuf, null)
      i += 1
    }
    keyBuf.writeTo(out)
    subOffset += keyBuf.size
    map.put(writeList.head, updateOffset)
    var rmCount = tree.root
    while (rmCount > 1) {
      rmCount -= 1
      keysList.remove(keysList.size - listOffsetFromEnd - rmCount)
    }
    subOffset
  }
}
