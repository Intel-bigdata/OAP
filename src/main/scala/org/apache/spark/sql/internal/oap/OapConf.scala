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

package org.apache.spark.sql.internal.oap

import org.apache.spark.sql.oap.adapter.SqlConfAdapter

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for OAP.
////////////////////////////////////////////////////////////////////////////////////////////////////


object OapConf {

  val OAP_ORC_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.orc.enable")
      .internal()
      .doc("Whether enable oap file format when encountering orc files")
      .booleanConf
      .createWithDefault(true)

  val OAP_PARQUET_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.parquet.enable")
      .internal()
      .doc("Whether enable oap file format when encountering parquet files")
      .booleanConf
      .createWithDefault(true)

  val OAP_FULL_SCAN_THRESHOLD =
    SqlConfAdapter.buildConf("spark.sql.oap.statistics.fullScanThreshold")
      .internal()
      .doc("Define the full scan threshold based on oap statistics in index file. " +
        "If the analysis result is above this threshold, it will full scan data file, " +
        "otherwise, follow index way.")
      .doubleConf
      .createWithDefault(0.2)

  val OAP_STATISTICS_TYPES =
    SqlConfAdapter.buildConf("spark.sql.oap.statistics.type")
      .internal()
      .doc("Which types of pre-defined statistics are added in index file. " +
        "And here you should just write the statistics name. " +
        "Now, three types statistics are supported. " +
        "\"MINMAX\" MinMaxStatistics, " +
        "\"SAMPLE\" for SampleBasedStatistics, " +
        "\"PARTBYVALUE\" for PartedByValueStatistics. " +
        "If you want to add more than one type, just use comma " +
        "to separate, eg. \"MINMAX, SAMPLE, PARTBYVALUE, BLOOM\"")
      .stringConf
      .transform(_.toUpperCase)
      .toSequence
      .transform(_.sorted)
      .checkValues(
        Set("MINMAX", "SAMPLE", "PARTBYVALUE", "BLOOM").subsets().map(_.toSeq.sorted).toSet)
      .createWithDefault(Seq("BLOOM", "MINMAX", "PARTBYVALUE", "SAMPLE"))

  val OAP_STATISTICS_PART_NUM =
    SqlConfAdapter.buildConf("spark.sql.oap.statistics.partNum")
      .internal()
      .doc("PartedByValueStatistics gives statistics with the value interval, default 5")
      .intConf
      .createWithDefault(5)

  val OAP_STATISTICS_SAMPLE_RATE =
    SqlConfAdapter.buildConf("spark.sql.oap.statistics.sampleRate")
      .internal()
      .doc("Sample rate for sample based statistics, default value 0.05")
      .doubleConf
      .createWithDefault(0.05)

  val OAP_STATISTICS_SAMPLE_MIN_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.statistics.sampleMinSize")
      .internal()
      .doc("Minimum sample size for Sample Statistics, default value 24")
      .intConf
      .createWithDefault(24)

  val OAP_BLOOMFILTER_MAXBITS =
    SqlConfAdapter.buildConf("spark.sql.oap.statistics.bloom.maxBits")
      .internal()
      .doc("Define the max bit count parameter used in bloom " +
        "filter, default 33554432")
      .intConf
      .createWithDefault(1 << 20)

  val OAP_BLOOMFILTER_NUMHASHFUNC =
    SqlConfAdapter.buildConf("spark.sql.oap.statistics.bloom.numHashFunc")
      .internal()
      .doc("Define the number of hash functions used in bloom filter, default 3")
      .intConf
      .createWithDefault(3)

  val OAP_FIBERCACHE_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.fiberCache.size")
      .internal()
      .doc("Define the size of fiber cache in KB, default 300 * 1024 KB")
      .longConf
      .createWithDefault(307200)

  val OAP_FIBERCACHE_STATS =
    SqlConfAdapter.buildConf("spark.sql.oap.fiberCache.stats")
      .internal()
      .doc("Whether enable cach stats record, default false")
      .booleanConf
      .createWithDefault(false)

  val OAP_FIBERCACHE_USE_OFFHEAP_RATIO =
    SqlConfAdapter.buildConf("spark.sql.oap.fiberCache.use.offheap.ratio")
      .internal()
      .doc("Define the ratio of fiber cache use 'spark.memory.offHeap.size' ratio.")
      .doubleConf
      .createWithDefault(0.7)

  val OAP_DATAFIBER_USE_FIBERCACHE_RATIO =
    SqlConfAdapter.buildConf("spark.sql.oap.dataCache.use.fiberCache.ratio")
      .internal()
      .doc("Define the ratio of data cache use fiber cache ratio. " +
        "This is not available under mix mode")
      .doubleConf
      .createWithDefault(0.8)

  val OAP_INDEX_DATA_SEPARATION_ENABLE =
    SqlConfAdapter.buildConf("spark.sql.oap.index.data.cache.separation.enable")
      .internal()
      .doc("This is to enable index and data cache separation feature for offheap and pm, " +
        "not for MixMemoryManager")
      .booleanConf
      .createWithDefault(false)

  val OAP_FIBERCACHE_MEMORY_MANAGER =
    SqlConfAdapter.buildConf("spark.sql.oap.fiberCache.memory.manager")
      .internal()
      .doc("Sets the implement of memory manager, it only supports offheap(DRAM OFF_HEAP) and " +
        "(PM) Intel Optane DC persistent memory currently.")
      .stringConf
      .createWithDefault("offheap")

  val OAP_MIX_INDEX_MEMORY_MANAGER =
    SqlConfAdapter.buildConf("spark.sql.oap.mix.index.memory.manager")
      .internal()
      .doc("Sets the implement of index memory manager in mix mode," +
        "it only supports offheap(DRAM OFF_HEAP) and " +
        "(PM) Intel Optane DC persistent memory currently." +
        "It should be different from spark.sql.oap.mix.data.memory.manager")
      .stringConf
      .createWithDefault("offheap")

  val OAP_MIX_DATA_MEMORY_MANAGER =
    SqlConfAdapter.buildConf("spark.sql.oap.mix.data.memory.manager")
      .internal()
      .doc("Sets the implement of data memory manager in mix mode," +
        "it only supports offheap(DRAM OFF_HEAP) and " +
        "(PM) Intel Optane DC persistent memory currently." +
        "It should be different from spark.sql.oap.mix.index.memory.manager")
      .stringConf
      .createWithDefault("pm")

  val OAP_FIBERCACHE_PERSISTENT_MEMORY_CONFIG_FILE =
    SqlConfAdapter.buildConf("spark.sql.oap.fiberCache.persistent.memory.config.file")
      .internal()
      .doc("A config file used to config the Intel Optane DC persistent memory initial path," +
        " and mapping with NUMA node.")
      .stringConf
      .createWithDefault("persistent-memory.xml")

  val OAP_FIBERCACHE_PERSISTENT_MEMORY_INITIAL_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.fiberCache.persistent.memory.initial.size")
      .internal()
      .doc("Used to set the initial size of Intel Optane DC persistent memory. The size is " +
        "used to control the maximum available persistent memory size used for each executor.")
      .stringConf
      .createWithDefault("0b")

  val OAP_FIBERCACHE_PERSISTENT_MEMORY_RESERVED_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.fiberCache.persistent.memory.reserved.size")
      .internal()
      .doc("Used to set the reserved size of Intel Optane DC persistent memory. Because the " +
        "heap management of Intel Optane DC persistent memory are based on jemalloc, so we " +
        "can't full use the total initial size memory. The reserved size should smaller than" +
        " initial size. Too small reserved size could result in OOM, too big size could reduce" +
        " the memory utilization rate.")
      .stringConf
      .createWithDefault("0b")

  val OAP_CACHE_FIBERSENSOR_GETHOSTS_NUM =
    SqlConfAdapter.buildConf("spark.sql.oap.cache.fiberSensor.getHostsNum")
      .internal()
      .doc("The length of getHosts function of FiberSensor's result Seq. The funcion returns " +
        "getHostsNum of hosts with the maximum FiberCache for certain filePath")
      .intConf
      .createWithDefault(3)

  val OAP_CACHE_FIBERSENSOR_MAXHOSTSMAINTAINED_NUM =
    SqlConfAdapter.buildConf("spark.sql.oap.cache.fiberSensor.maxHostsMaintainedNum")
      .internal()
      .doc("The maximum maintained number of hosts number for a certain filePath in FiberSensor")
      .intConf
      .createWithDefault(10)

  val OAP_COMPRESSION = SqlConfAdapter.buildConf("spark.sql.oap.compression.codec")
    .internal()
    .doc("Sets the compression codec use when writing Parquet files. Acceptable values include: " +
      "uncompressed, snappy, gzip, lzo.")
    .stringConf
    .transform(_.toUpperCase())
    .checkValues(Set("UNCOMPRESSED", "SNAPPY", "GZIP", "LZO"))
    .createWithDefault("GZIP")

  val OAP_INDEX_BTREE_COMPRESSION =
    SqlConfAdapter.buildConf("spark.sql.oap.index.compression.codec")
    .internal()
    .doc("Sets the compression codec use when writing Parquet files. Acceptable values include: " +
        "uncompressed, snappy, gzip, lzo.")
    .stringConf
    .transform(_.toUpperCase())
    .checkValues(Set("UNCOMPRESSED", "SNAPPY", "GZIP", "LZO"))
    .createWithDefault("GZIP")

  val OAP_ROW_GROUP_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.rowgroup.size")
      .internal()
      .doc("Define the row number for each row group")
      .intConf
      .createWithDefault(1024 * 1024)

  val OAP_ENABLE_OINDEX =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.enabled")
      .internal()
      .doc("To indicate to enable/disable oindex for developers even if the index file is there")
      .booleanConf
      .createWithDefault(true)

  val OAP_ENABLE_EXECUTOR_INDEX_SELECTION =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.eis.enabled")
      .internal()
      .doc("To indicate if enable/disable index cbo which helps to choose a fast query path")
      .booleanConf
      .createWithDefault(true)

  val OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.file.policy")
      .internal()
      .doc("To indicate if enable/disable file based index selection")
      .booleanConf
      .createWithDefault(true)

  val OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.statistics.policy")
      .internal()
      .doc("To indicate if enable/disable statistics based index selection")
      .booleanConf
      .createWithDefault(true)

  val OAP_ENABLE_OPTIMIZATION_STRATEGIES =
    SqlConfAdapter.buildConf("spark.sql.oap.strategies.enabled")
      .internal()
      .doc("To indicate if enable/disable oap strategies")
      .booleanConf
      .createWithDefault(false)

  val OAP_INDEX_FILE_SIZE_MAX_RATIO =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.size.ratio")
      .internal()
      .doc("To indicate if enable/disable index cbo which helps to choose a fast query path")
      .doubleConf
      .createWithDefault(0.7)

  val OAP_INDEXER_CHOICE_MAX_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.indexer.max.use.size")
      .internal()
      .doc("The max availabe indexer choose size.")
      .intConf
      .createWithDefault(1)

  val OAP_INDEXER_USE_CONSTANT_TIMESTAMPS_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.use.constant.timestamps")
      .internal()
      .doc("To indicate if enable/disable use constant timestamps when create oindex")
      .booleanConf
      .createWithDefault(false)

  val OAP_INDEXER_TIMESTAMPS_CONSTANT =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.timestamps.constant")
      .internal()
      .doc("If 'spark.sql.oap.oindex.use.constant.timestamps' is true, use this value to fill " +
        "index meta timestamps")
      .longConf
      .createWithDefault(1555299314969L)

  val OAP_BTREE_ROW_LIST_PART_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.btree.rowList.part.size")
      .internal()
      .doc("The row count of each part of row list in btree index")
      .intConf
      .createWithDefault(1024 * 1024)

  val OAP_INDEX_DISABLE_LIST =
    SqlConfAdapter.buildConf("spark.sql.oap.oindex.disable.list")
    .internal()
    .doc("To disable specific index by index names for test purpose, this is supposed to be in " +
      "the format of indexA,indexB,indexC")
    .stringConf
    .createWithDefault("")

  val OAP_HEARTBEAT_INTERVAL =
    SqlConfAdapter.buildConf("spark.sql.oap.heartbeatInterval")
    .internal()
    .doc("To Configure the OAP status update interval, for example OAP metrics")
    .stringConf
    .createWithDefault("2s")

  val OAP_UPDATE_FIBER_CACHE_METRICS_INTERVAL_SEC =
    SqlConfAdapter.buildConf("spark.sql.oap.update.fiber.cache.metrics.interval.sec")
      .internal()
      .doc("The interval of fiber cache metrics update")
      .longConf
      .createWithDefault(10L)

  val OAP_INDEX_BTREE_WRITER_VERSION =
    SqlConfAdapter.buildConf("spark.sql.oap.index.btree.writer.version")
      .internal()
      .doc("The writer version of BTree index")
      .stringConf
      .checkValues(Set("v1", "v2"))
      .createWithDefault("v1")

  val OAP_PARQUET_DATA_CACHE_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.parquet.data.cache.enable")
      .internal()
      .doc("To indicate if enable parquet data cache, default false")
      .booleanConf
      .createWithDefault(false)

  val OAP_ORC_DATA_CACHE_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.orc.data.cache.enable")
      .internal()
      .doc("To indicate if enable orc data cache, default false")
      .booleanConf
      .createWithDefault(false)

  val OAP_PARQUET_INDEX_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.parquet.index.enable")
      .internal()
      .doc("To indicate if enable parquet index, default true")
      .booleanConf
      .createWithDefault(true)

  val OAP_INDEX_DIRECTORY =
    SqlConfAdapter.buildConf("spark.sql.oap.index.directory")
      .internal()
      .doc("To specify the directory of index file, if the value " +
        "is empty, it will store in the data file path")
      .stringConf
      .createWithDefault("")

  val OAP_INDEX_STATISTIC_EXTERNALSORTER_ENABLE =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistic.externalsorter.enable")
      .internal()
      .doc("To indicate if to enable externalsorter for statistic calculation")
      .booleanConf
      .createWithDefault(true)

  val OAP_ENABLE_DATA_FIBER_CACHE_COMPRESSION =
    SqlConfAdapter.buildConf("spark.sql.oap.data.fiber.cache.compress.enable")
      .internal()
      .doc("To indicate if enable/disable data fiber cache compression")
      .booleanConf
      .createWithDefault(false)

  val OAP_DATA_FIBER_CACHE_COMPRESSION_CODEC =
    SqlConfAdapter.buildConf("spark.sql.oap.data.fiber.cache.compression.codec")
      .internal()
      .doc("Sets the compression codec use when writing data fiber cache." +
        " Acceptable values include: LZ4, LZF, SNAPPY, ZSTD.")
      .stringConf
      .transform(_.toUpperCase())
      .checkValues(Set("LZ4", "LZF", "SNAPPY", "ZSTD"))
      .createWithDefault("LZ4")

  val OAP_DATA_FIBER_CACHE_COMPRESSION_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.data.fiber.cache.compression.size")
      .internal()
      .doc("The oap data fiber compression unit length")
      .intConf
      .createWithDefault(4096)
}
