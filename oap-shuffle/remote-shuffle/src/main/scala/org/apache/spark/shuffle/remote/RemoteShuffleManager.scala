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

package org.apache.spark.shuffle.remote

import java.io.IOException
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.remote.RemoteShuffleManager.{active, appendRemoteStorageHadoopConfigurations}
import org.apache.spark.shuffle.sort._

/**
  * In remote shuffle, data is written to a remote Hadoop compatible file system instead of local
  * disks.
  */
private[spark] class RemoteShuffleManager(private val conf: SparkConf) extends ShuffleManager with
    Logging {

  require(conf.get(
    config.SHUFFLE_SERVICE_ENABLED.key, config.SHUFFLE_SERVICE_ENABLED.defaultValueString)
      == "false", "Remote shuffle and external shuffle service: they cannot be enabled at the" +
      " same time")

  RemoteShuffleManager.setActive(this)

  logWarning("******** Remote Shuffle Manager is used ********")

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
          " Shuffle will continue to spill to disk when necessary.")
  }

  /**
    * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override val shuffleBlockResolver = new RemoteShuffleBlockResolver(conf)

  /**
    * Obtains a [[ShuffleHandle]] to pass to tasks.
    */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (RemoteShuffleManager.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (RemoteShuffleManager.canUseSerializedShuffle(dependency, conf)) {
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
    * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
    * Called on executors by reduce tasks.
    */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new RemoteShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      shuffleBlockResolver,
      startPartition,
      endPartition,
      context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new RemoteUnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId, context, env.conf)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new RemoteBypassMergeSortShuffleWriter(
          env.blockManager,
          shuffleBlockResolver,
          bypassMergeSortHandle,
          mapId,
          context,
          env.conf)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new RemoteShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  private[spark] val getHadoopConf = {
    val storageMasterUri = active.conf.get("spark.shuffle.remote.storageMasterUri")

    // DAOS-Hadoop-compatible-filesystem configurations are loaded by DAOS Filesystem itself
    val hadoopConf = new Configuration(false)
    // Hadoop configuration will be loaded from a remote web URI if the shuffle storage
    // system is HDFS
    if (storageMasterUri.startsWith("hdfs")) {
      val host = storageMasterUri.split("//")(1).split(":")(0)
      val port = active.conf.get(RemoteShuffleConf.STORAGE_HDFS_MASTER_UI_PORT)
      val address = s"http://$host:$port/conf"
      try {
        hadoopConf.addResource(new URL(address).openConnection.getInputStream)
      } catch {
        // Suppress this Exception and use the default one
        case e: IOException => logWarning(
          s"Exception occurs getting configurations from: $address, caused by  ${e.getMessage}")
      }
    }

    (new SparkHadoopUtil).appendS3AndSparkHadoopConfigurations(active.conf, hadoopConf)
    appendRemoteStorageHadoopConfigurations(active.conf, hadoopConf)
    hadoopConf
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object RemoteShuffleManager extends Logging {

  var active: RemoteShuffleManager = _
  private[remote] def setActive(update: RemoteShuffleManager): Unit = active = update

  private def appendRemoteStorageHadoopConfigurations(
      sparkConf: SparkConf, hadoopConf: Configuration) = {
    hadoopConf.set("dfs.replication", sparkConf.get(RemoteShuffleConf.DFS_REPLICATION).toString)
  }

  def getFileSystem : FileSystem = {
    require(active != null,
      "Active RemoteShuffleManager unassigned! It's probably never constructed")
    active.shuffleBlockResolver.fs
  }

  def getResolver: RemoteShuffleBlockResolver = {
    require(active != null,
      "Active RemoteShuffleManager unassigned! It's probably never constructed")
    active.shuffleBlockResolver
  }

  def getConf: SparkConf = {
    require(active != null,
      "Active RemoteShuffleManager unassigned! It's probably never constructed")
    active.conf
  }

  /**
    * Make the decision also referring to a configuration
    */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _], conf: SparkConf): Boolean = {
    val optimizedShuffleEnabled = conf.get(RemoteShuffleConf.REMOTE_OPTIMIZED_SHUFFLE_ENABLED)
    optimizedShuffleEnabled && SortShuffleManager.canUseSerializedShuffle(dependency)
  }

  // This is identical to [[SortShuffleWriter.shouldBypassMergeSort]], except reading from
  // a modified configuration name, due to we need to change the default threshold to -1 in remote
  // shuffle
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {

    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      false
    } else {
      val bypassMergeThreshold = conf.get(RemoteShuffleConf.REMOTE_BYPASS_MERGE_THRESHOLD)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
