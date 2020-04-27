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

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object RemoteShuffleConf {

  val STORAGE_MASTER_URI: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.remote.storageMasterUri")
      .doc("Contact this storage master while persisting shuffle files")
      .stringConf
      .createWithDefault("hdfs://localhost:9001")

  val STORAGE_HDFS_MASTER_UI_PORT: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.remote.hdfs.storageMasterUIPort")
      .doc("Contact this UI port to retrieve HDFS configurations")
      .stringConf
      .createWithDefault("50070")

  val SHUFFLE_FILES_ROOT_DIRECTORY: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.remote.filesRootDirectory")
      .doc("Use this as the root directory for shuffle files")
      .stringConf
      .createWithDefault("/shuffle")

  val DFS_REPLICATION: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.remote.hdfs.replication")
      .doc("The default replication of remote storage system, will override dfs.replication" +
        " when HDFS is used as shuffling storage")
      .intConf
      .createWithDefault(3)

  val REMOTE_OPTIMIZED_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.remote.optimizedPathEnabled")
      .doc("Enable using unsafe-optimized shuffle writer")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val REMOTE_BYPASS_MERGE_THRESHOLD: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.remote.bypassMergeThreshold")
      .doc("Remote shuffle manager uses this threshold to decide using bypass-merge(hash-based)" +
        "shuffle or not, a new configuration is introduced(and it's -1 by default) because we" +
        " want to explicitly make disabling hash-based shuffle writer as the default behavior." +
        " When memory is relatively sufficient, using sort-based shuffle writer in remote shuffle" +
        " is often more efficient than the hash-based one. Because the bypass-merge shuffle " +
        "writer proceeds I/O of 3x total shuffle size: 1 time for read I/O and 2 times for write" +
        " I/Os, and this can be an even larger overhead under remote shuffle, the 3x shuffle size" +
        " is gone through network, arriving at remote storage system.")
      .intConf
      .createWithDefault(-1)

  val REMOTE_INDEX_CACHE_SIZE: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.remote.index.cache.size")
      .doc("This index file cache resides in each executor. If it's a positive value, index " +
        "cache will be turned on: instead of reading index files directly from remote storage" +
        ", a reducer will fetch the index files from the executors that write them through" +
        " network. And those executors will return the index files kept in cache. (read them" +
        "from storage if needed)")
      .stringConf
      .createWithDefault("0")

  val NUM_TRANSFER_SERVICE_THREADS: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.remote.numIndexReadThreads")
      .doc("The maximum number of server/client threads used in RemoteShuffleTransferService for" +
        "index files transferring")
      .intConf
      .createWithDefault(3)

  val NUM_CONCURRENT_FETCH: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.remote.numReadThreads")
      .doc("The maximum number of concurrent reading threads fetching shuffle data blocks")
      .intConf
      .createWithDefault(Runtime.getRuntime.availableProcessors())

  val REUSE_FILE_HANDLE: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.remote.reuseFileHandle")
      .doc("By switching on this feature, the file handles returned by Filesystem open operations" +
        " will be cached/reused inside an executor(across different rounds of reduce tasks)," +
        " eliminating open overhead. This should improve the reduce stage performance only when" +
        " file open operations occupy majority of the time, e.g. There is a large number of" +
        " shuffle blocks, each reading a fairly small block of data, and there is no other" +
        " compute in the reduce stage.")
      .booleanConf
      .createWithDefault(false)

  val DATA_FETCH_EAGER_REQUIREMENT: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.remote.eagerRequirementDataFetch")
      .doc("With eager requirement = false, a shuffle block will be counted ready and served for" +
        " compute until all content of the block is put in Spark's local memory. With eager " +
        "requirement = true, a shuffle block will be served to later compute after the bytes " +
        "required is fetched and put in memory")
      .booleanConf
      .createWithDefault(false)

}
