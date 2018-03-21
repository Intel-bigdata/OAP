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

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for OAP.
////////////////////////////////////////////////////////////////////////////////////////////////////


object OapConf {
  import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder

  val OAP_PARQUET_ENABLED =
    SQLConfigBuilder("spark.sql.oap.parquet.enable")
      .internal()
      .doc("Whether enable oap file format when encounter parquet files")
      .booleanConf
      .createWithDefault(true)

  val OAP_FULL_SCAN_THRESHOLD =
    SQLConfigBuilder("spark.sql.oap.statistics.fullScanThreshold")
      .internal()
      .doc("Define the full scan threshold based on oap statistics in index file. " +
        "If the analysis result is above this threshold, it will full scan data file, " +
        "otherwise, follow index way.")
      .doubleConf
      .createWithDefault(0.2)

  val OAP_STATISTICS_TYPES =
    SQLConfigBuilder("spark.sql.oap.statistics.type")
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
    SQLConfigBuilder("spark.sql.oap.statistics.partNum")
      .internal()
      .doc("PartedByValueStatistics gives statistics with the value interval, default 5")
      .intConf
      .createWithDefault(5)

  val OAP_STATISTICS_SAMPLE_RATE =
    SQLConfigBuilder("spark.sql.oap.statistics.sampleRate")
      .internal()
      .doc("Sample rate for sample based statistics, default value 0.05")
      .doubleConf
      .createWithDefault(0.05)

  val OAP_BLOOMFILTER_MAXBITS =
    SQLConfigBuilder("spark.sql.oap.statistics.bloom.maxBits")
      .internal()
      .doc("Define the max bit count parameter used in bloom " +
        "filter, default 33554432")
      .intConf
      .createWithDefault(1 << 20)

  val OAP_BLOOMFILTER_NUMHASHFUNC =
    SQLConfigBuilder("spark.sql.oap.statistics.bloom.numHashFunc")
      .internal()
      .doc("Define the number of hash functions used in bloom filter, default 3")
      .intConf
      .createWithDefault(3)

  val OAP_FIBERCACHE_SIZE =
    SQLConfigBuilder("spark.sql.oap.fiberCache.size")
      .internal()
      .doc("Define the size of fiber cache in KB, default 300 * 1024 KB")
      .longConf
      .createWithDefault(307200)

  val OAP_FIBERCACHE_STATS =
    SQLConfigBuilder("spark.sql.oap.fiberCache.stats")
      .internal()
      .doc("Whether enable cach stats record, default false")
      .booleanConf
      .createWithDefault(false)

  val OAP_COMPRESSION = SQLConfigBuilder("spark.sql.oap.compression.codec")
    .internal()
    .doc("Sets the compression codec use when writing Parquet files. Acceptable values include: " +
      "uncompressed, snappy, gzip, lzo.")
    .stringConf
    .transform(_.toUpperCase())
    .checkValues(Set("UNCOMPRESSED", "SNAPPY", "GZIP", "LZO"))
    .createWithDefault("GZIP")

  val OAP_ROW_GROUP_SIZE =
    SQLConfigBuilder("spark.sql.oap.rowgroup.size")
      .internal()
      .doc("Define the row number for each row group")
      .intConf
      .createWithDefault(1024 * 1024)

  val OAP_ENABLE_OINDEX =
    SQLConfigBuilder("spark.sql.oap.oindex.enabled")
      .internal()
      .doc("To indicate to enable/disable oindex for developers even if the index file is there")
      .booleanConf
      .createWithDefault(true)

  val OAP_ENABLE_EXECUTOR_INDEX_SELECTION =
    SQLConfigBuilder("spark.sql.oap.oindex.eis.enabled")
      .internal()
      .doc("To indicate if enable/disable index cbo which helps to choose a fast query path")
      .booleanConf
      .createWithDefault(true)

  val OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY =
    SQLConfigBuilder("spark.sql.oap.oindex.file.policy")
      .internal()
      .doc("To indicate if enable/disable file based index selection")
      .booleanConf
      .createWithDefault(true)

  val OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY =
    SQLConfigBuilder("spark.sql.oap.oindex.statistics.policy")
      .internal()
      .doc("To indicate if enable/disable statistics based index selection")
      .booleanConf
      .createWithDefault(true)

  val OAP_ENABLE_OPTIMIZATION_STRATEGIES =
    SQLConfigBuilder("spark.sql.oap.strategies.enabled")
      .internal()
      .doc("To indicate if enable/disable oap strategies")
      .booleanConf
      .createWithDefault(false)

  val OAP_INDEX_FILE_SIZE_MAX_RATIO =
    SQLConfigBuilder("spark.sql.oap.oindex.size.ratio")
      .internal()
      .doc("To indicate if enable/disable index cbo which helps to choose a fast query path")
      .doubleConf
      .createWithDefault(0.7)

  val OAP_INDEXER_CHOICE_MAX_SIZE =
    SQLConfigBuilder("spark.sql.oap.indexer.max.use.size")
      .internal()
      .doc("The max availabe indexer choose size.")
      .intConf
      .createWithDefault(1)

  val OAP_BTREE_ROW_LIST_PART_SIZE =
    SQLConfigBuilder("spark.sql.oap.btree.rowList.part.size")
      .internal()
      .doc("The row count of each part of row list in btree index")
      .intConf
      .createWithDefault(1024 * 1024)

  val OAP_INDEX_DISABLE_LIST =
    SQLConfigBuilder("spark.sql.oap.oindex.disable.list")
    .internal()
    .doc("To disable specific index by index names for test purpose, this is supposed to be in " +
      "the format of indexA,indexB,indexC")
    .stringConf
    .createWithDefault("")

  val OAP_UPDATE_FIBER_CACHE_METRICS_INTERVAL_SEC =
    SQLConfigBuilder("spark.sql.oap.update.fiber.cache.metrics.interval.sec")
      .internal()
      .doc("The interval of fiber cache metrics update")
      .longConf
      .createWithDefault(10L)
}
