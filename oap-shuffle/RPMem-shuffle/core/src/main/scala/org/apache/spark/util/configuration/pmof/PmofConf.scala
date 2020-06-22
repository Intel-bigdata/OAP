package org.apache.spark.util.configuration.pmof

import org.apache.spark.SparkConf

class PmofConf(conf: SparkConf) {
  val enableRdma: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_rdma", defaultValue = true)
  val enablePmem: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_pmem", defaultValue = true)
  val path_list: List[String] = conf.get("spark.shuffle.pmof.pmem_list", defaultValue = "/dev/dax0.0").split(",").map(_.trim).toList
  val maxPoolSize: Long = conf.getLong("spark.shuffle.pmof.pmpool_size", defaultValue = 1073741824)
  val maxStages: Int = conf.getInt("spark.shuffle.pmof.max_stage_num", defaultValue = 1000)
  val maxMaps: Int = conf.getInt("spark.shuffle.pmof.max_map_num", defaultValue = 1000)
  val spill_throttle: Long = conf.getLong("spark.shuffle.pmof.spill_throttle", defaultValue = 4194304)
  val inMemoryCollectionSizeThreshold: Long =
    conf.getLong("spark.shuffle.spill.pmof.MemoryThreshold", 5 * 1024 * 1024)
  val networkBufferSize: Int = conf.getInt("spark.shuffle.pmof.network_buffer_size", 4096 * 3)
  val driverHost: String = conf.get("spark.driver.rhost", defaultValue = "172.168.0.43")
  val driverPort: Int = conf.getInt("spark.driver.rport", defaultValue = 61000)
  val serverBufferNums: Int = conf.getInt("spark.shuffle.pmof.server_buffer_nums", 256)
  val serverWorkerNums = conf.getInt("spark.shuffle.pmof.server_pool_size", 1)
  val clientBufferNums: Int = conf.getInt("spark.shuffle.pmof.client_buffer_nums", 16)
  val clientWorkerNums = conf.getInt("spark.shuffle.pmof.server_pool_size", 1)
  val shuffleNodes: Array[Array[String]] =
    conf.get("spark.shuffle.pmof.node", defaultValue = "").split(",").map(_.split("-"))
  val map_serializer_buffer_size = conf.getLong("spark.shuffle.pmof.map_serializer_buffer_size", 16 * 1024)
  val reduce_serializer_buffer_size = conf.getLong("spark.shuffle.pmof.reduce_serializer_buffer_size", 16 * 1024)
  val metadataCompress: Boolean = conf.getBoolean("spark.shuffle.pmof.metadata_compress", defaultValue = false)
  val shuffleBlockSize: Int = conf.getInt("spark.shuffle.pmof.shuffle_block_size", defaultValue = 2048)
  val pmemCapacity: Long = conf.getLong("spark.shuffle.pmof.pmem_capacity", defaultValue = 264239054848L)
  val pmemCoreMap = conf.get("spark.shuffle.pmof.dev_core_set", defaultValue = "/dev/dax0.0:0-17,36-53").split(";").map(_.trim).map(_.split(":")).map(arr => arr(0) -> arr(1)).toMap
}
