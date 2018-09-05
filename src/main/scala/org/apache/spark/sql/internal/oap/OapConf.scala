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

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.oap.adapter.SqlConfAdapter

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for OAP.
////////////////////////////////////////////////////////////////////////////////////////////////////


object OapConf {

  val OAP_PARQUET_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.parquet.enabled")
      .internal()
      .doc("Whether enable oap file format when encounter parquet files")
      .booleanConf
      .createWithDefault(true)

  val OAP_INDEX_STATISTICS_FULL_SCAN_THRESHOLD =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistics.fullScanThreshold")
      .internal()
      .doc("Define the full scan threshold based on oap statistics in index file. " +
        "If the analysis result is above this threshold, it will full scan data file, " +
        "otherwise, follow index way.")
      .doubleConf
      .createWithDefault(0.2)

  val OAP_INDEX_STATISTICS_TYPES =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistics.types")
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

  val OAP_INDEX_STATISTICS_PART_NUM =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistics.partNum")
      .internal()
      .doc("PartedByValueStatistics gives statistics with the value interval, default 5")
      .intConf
      .createWithDefault(5)

  val OAP_INDEX_STATISTICS_SAMPLE_RATE =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistics.sampleRate")
      .internal()
      .doc("Sample rate for sample based statistics, default value 0.05")
      .doubleConf
      .createWithDefault(0.05)

  val OAP_INDEX_STATISTICS_SAMPLE_MIN_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistics.sampleMinSize")
      .internal()
      .doc("Minimum sample size for Sample Statistics, default value 24")
      .intConf
      .createWithDefault(24)

  val OAP_INDEX_STATISTICS_BLOOMFILTER_MAXBITS =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistics.bloomFilter.maxBits")
      .internal()
      .doc("Define the max bit count parameter used in bloom " +
        "filter, default 33554432")
      .intConf
      .createWithDefault(1 << 20)

  val OAP_INDEX_STATISTICS_BLOOMFILTER_NUMHASHFUNC =
    SqlConfAdapter.buildConf("spark.sql.oap.index.statistics.bloomFilter.numHashFunc")
      .internal()
      .doc("Define the number of hash functions used in bloom filter, default 3")
      .intConf
      .createWithDefault(3)

  val OAP_CACHE_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.cache.size")
      .internal()
      .doc("Define the size of fiber cache in KB, default 300 * 1024 KB")
      .longConf
      .createWithDefault(307200)

  val OAP_CACHE_STATS =
    SqlConfAdapter.buildConf("spark.sql.oap.cache.stats")
      .internal()
      .doc("Whether enable cach stats record, default false")
      .booleanConf
      .createWithDefault(false)

  val OAP_CACHE_OFFHEAP_RATIO =
    SqlConfAdapter.buildConf("spark.sql.oap.cache.offheapRatio")
      .internal()
      .doc("Define the ratio of fiber cache use 'spark.memory.offHeap.size' ratio.")
      .doubleConf
      .createWithDefault(0.7)

  val OAP_CACHE_MEMORY_MANAGER =
    SqlConfAdapter.buildConf("spark.sql.oap.cache.memoryManager")
      .internal()
      .doc("Sets the implementation of memory manager, it only supports off heap currently.")
      .stringConf
      .createWithDefault("offheap")

  val OAP_IO_COMPRESSION = SqlConfAdapter.buildConf("spark.sql.oap.io.compression.codec")
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

  val OAP_OAPFILEFORMAT_ROWGROUP_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.oapFileFormat.rowGroupSize")
      .internal()
      .doc("Define the row number for each row group")
      .intConf
      .createWithDefault(1024 * 1024)

  val OAP_INDEX_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.index.enabled")
      .internal()
      .doc("Whether directly ignore OAP index files even if they exist")
      .booleanConf
      .createWithDefault(true)

  val OAP_INDEX_ENABLE_EXECUTOR_SELECTION =
    SqlConfAdapter.buildConf("spark.sql.oap.index.eis.enabled")
      .internal()
      .doc("To indicate if enable/disable index cbo which helps to choose a fast query path")
      .booleanConf
      .createWithDefault(true)

  val OAP_INDEX_ENABLE_EXECUTOR_SELECTION_FILE_POLICY =
    SqlConfAdapter.buildConf("spark.sql.oap.index.eis.filePolicyEnabled")
      .internal()
      .doc("To indicate if enable/disable file based index selection")
      .booleanConf
      .createWithDefault(true)

  val OAP_INDEX_ENABLE_EXECUTOR_SELECTION_STATISTICS_POLICY =
    SqlConfAdapter.buildConf("spark.sql.oap.index.eis.statisticsPolicyEnabled")
      .internal()
      .doc("To indicate if enable/disable statistics based index selection")
      .booleanConf
      .createWithDefault(true)

  val OAP_STRATEGY_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.strategy.enabled")
      .internal()
      .doc("To indicate if enable/disable oap strategies")
      .booleanConf
      .createWithDefault(false)

  val OAP_INDEX_FILE_SIZE_MAX_RATIO =
    SqlConfAdapter.buildConf("spark.sql.oap.index.size.ratio")
      .internal()
      .doc("To indicate if enable/disable index cbo which helps to choose a fast query path")
      .doubleConf
      .createWithDefault(0.7)

  val OAP_INDEX_INDEXER_CHOICE_MAX_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.index.indexerMaxChooseSize")
      .internal()
      .doc("The max available indexer choose size.")
      .intConf
      .createWithDefault(1)

  val OAP_INDEX_BTREE_ROW_LIST_PART_SIZE =
    SqlConfAdapter.buildConf("spark.sql.oap.index.btree.rowListPartSize")
      .internal()
      .doc("The row count of each part of row list in btree index")
      .intConf
      .createWithDefault(1024 * 1024)

  val OAP_INDEX_DISABLE_LIST =
    SqlConfAdapter.buildConf("spark.sql.oap.index.disableList")
    .internal()
    .doc("To disable specific index by index names for test purpose, this is supposed to be in " +
      "the format of indexA,indexB,indexC")
    .stringConf
    .createWithDefault("")

  val OAP_RPC_HEARTBEAT_INTERVAL =
    SqlConfAdapter.buildConf("spark.sql.oap.rpc.heartbeatInterval")
    .internal()
    .doc("To Configure the OAP status update interval, for example OAP metrics")
    .stringConf
    .createWithDefault("2s")

  val OAP_METRICS_UPDATE_FIBER_CACHE_INTERVAL_SEC =
    SqlConfAdapter.buildConf("spark.sql.oap.metrics.updateFiberCacheIntervalSec")
      .internal()
      .doc("The interval of fiber cache metrics update")
      .longConf
      .createWithDefault(10L)

  val OAP_INDEX_BTREE_WRITER_VERSION =
    SqlConfAdapter.buildConf("spark.sql.oap.index.btree.writerVersion")
      .internal()
      .doc("The writer version of BTree index")
      .stringConf
      .checkValues(Set("v1", "v2"))
      .createWithDefault("v1")

  val OAP_CACHE_PARQUET_DATA_FILE_ENABLED =
    SqlConfAdapter.buildConf("spark.sql.oap.cache.parquetDataFileEnabled")
      .internal()
      .doc("To indicate if enable parquet data cache, default false")
      .booleanConf
      .createWithDefault(false)

  val OAP_INDEX_DIRECTORY =
    SqlConfAdapter.buildConf("spark.sql.oap.index.directory")
      .internal()
      .doc("To specify the directory of index file, if the value is empty, it will store in the" +
        " data file path")
      .stringConf
      .createWithDefault("")
}
