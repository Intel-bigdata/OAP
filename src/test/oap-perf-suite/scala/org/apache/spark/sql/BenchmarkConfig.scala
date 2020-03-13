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
package org.apache.spark.sql

import scala.collection.mutable

// TODO: use SQLConf style i.e. (value, defaultValue)
class BenchmarkConfig {
  // Benchmark config, include file format, index use or not, etc.
  private val benchmarkConf: mutable.HashMap[String, String] = mutable.HashMap.empty

  // Spark conf, to initial spark session.
  private val sparkConf: mutable.HashMap[String, String] = mutable.HashMap.empty

  def setBenchmarkConf(name: String, value: String): BenchmarkConfig = {
    benchmarkConf.put(name, value)
    this
  }

  /** A meaningful name for this config
   * like "oap + index" or "parquet w/o index" or "oap and oapStrategy enable"
   */
  def setBenchmarkConfName(name: String): BenchmarkConfig = {
    confName = Option(name)
    this
  }

  var confName: Option[String] = None

  def setSparkConf(name: String, value: String): BenchmarkConfig = {
    sparkConf.put(name, value)
    this
  }

  /**
   *  Find a conf from all conf settings.
   */
  def getConf(name: String): String = {
    benchmarkConf.get(name).getOrElse(
      sparkConf.get(name).getOrElse(
        s"$name Not Exist!!!"))
  }

  /**
   * Get benchmark config
   * @param name: name
   * @return benchmark config setting.
   */
  def getBenchmarkConf(name: String): String = benchmarkConf.getOrElse(name, "false")

  /**
   * Get spark config
   * @param name: name
   * @return sql config setting.
   */
  def getSparkConf(name: String): String = sparkConf.getOrElse(name, "false")

  /**
   * Get all spark config
   * @return all spark config settings.
   */
  def allSparkOptions(): Map[String, String] = sparkConf.toMap[String, String]

  /**
   * Make config settings as config name, used if none name set.
   * @return
   */
  def configString: String = {
    if (sparkConf.isEmpty) {
      val indexEnable = if (getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE).toBoolean) {
        "W/ Index"
      } else {
        "W/O Index"
      }

      s"${getBenchmarkConf(BenchmarkConfig.FILE_FORMAT)} $indexEnable"
    } else {
      // oap !eis & statistics
      getBenchmarkConf(BenchmarkConfig.FILE_FORMAT) + " "
      sparkConf.toArray.map{ setting =>
        val flag = if (setting._2 == "true") {
          ""
        } else {
          "!"
        }
        flag + setting._1.split('.')(4)
      }.mkString(getBenchmarkConf(BenchmarkConfig.FILE_FORMAT) + " ", " & ", "")
    }
  }

  override def toString: String = {
    confName match {
      case Some(name) => name
      case None => configString
    }
  }
}

object BenchmarkConfig {
  val INDEX_ENABLE = "oap.benchmark.config.index"
  val FILE_FORMAT  = "oap.benchmark.config.format"
}

abstract class BenchmarkConfigSelector {
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig]
}

object BenchmarkConfigSelector {
  // TODO: build config accordingly.
  val wildcardConfiguration: mutable.HashMap[String, String] = mutable.HashMap.empty

  def build(options: Map[String, String]): Unit = {
    wildcardConfiguration ++= options
  }

  def isSelected(config: BenchmarkConfig): Boolean = {
    if (wildcardConfiguration.nonEmpty) {
      wildcardConfiguration.exists{conf =>
        config.getConf(conf._1) == conf._2 ||
        config.confName.equals(conf._2)
      }
    } else {
      true
    }
  }
}

trait ParquetOnlyConfigSet extends BenchmarkConfigSelector{
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig] = Seq(
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/ index")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
  )
}

trait OrcOnlyConfigSet extends BenchmarkConfigSelector{
  def allConfigurations: Seq[BenchmarkConfig] = Seq(
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/ index")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/o index")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
  )
}

trait ParquetVsOapVsOrcConfigSet extends BenchmarkConfigSelector{
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig] = Seq(
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/ index")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/ index oap cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.orc.data.cache.enable", "true")
      .setSparkConf("spark.sql.orc.copyBatchToSpark", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/ index oap binary cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.orc.binary.cache.enable", "true")
      .setSparkConf("spark.sql.orc.copyBatchToSpark", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/ index data cache separation enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.orc.data.cache.enable", "true")
      .setSparkConf("spark.sql.orc.copyBatchToSpark", "true")
      .setSparkConf("spark.sql.oap.index.data.cache.separation.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/ index data binary cache separation enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.orc.binary.cache.enable", "true")
      .setSparkConf("spark.sql.orc.copyBatchToSpark", "true")
      .setSparkConf("spark.sql.oap.index.data.cache.separation.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/o index")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/o index oap cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.orc.data.cache.enable", "true")
      .setSparkConf("spark.sql.orc.copyBatchToSpark", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("Orc w/o index oap binary cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "orc")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.orc.binary.cache.enable", "true")
      .setSparkConf("spark.sql.orc.copyBatchToSpark", "true"),
//    new BenchmarkConfig()
//      .setBenchmarkConfName("oap w/ index")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
//    new BenchmarkConfig()
//      .setBenchmarkConfName("oap w/ index oap cache enabled")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
//      .setSparkConf("spark.sql.oap.oapfileformat.data.cache.enable", "true"),
//    new BenchmarkConfig()
//      .setBenchmarkConfName("oap w/ index data cache separation enabled")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
//      .setSparkConf("spark.sql.oap.oapfileformat.data.cache.enable", "true")
//      .setSparkConf("spark.sql.oap.index.data.cache.separation.enable", "true"),
//    new BenchmarkConfig()
//      .setBenchmarkConfName("oap w/o index")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false"),
//    new BenchmarkConfig()
//      .setBenchmarkConfName("oap w/o index oap cache enabled")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
//      .setSparkConf("spark.sql.oap.oapfileformat.data.cache.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/ index oap cache disabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/ index oap cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/ index oap binary cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.parquet.binary.cache.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap cache disabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap binary cache enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.binary.cache.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/ index data cache separation enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true")
      .setSparkConf("spark.sql.oap.index.data.cache.separation.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/ index data binary cache separation enabled")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true")
      .setSparkConf("spark.sql.oap.index.binary.cache.separation.enable", "true"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap cache enabled with LZ4 cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "LZ4"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap binary cache enabled with LZ4 cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.binary.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "LZ4"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap cache enabled with LZF cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "LZF"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap binary cache enabled with LZF cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.binary.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "LZF"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap cache enabled with SNAPPY cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "SNAPPY"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap binary cache enabled with SNAPPY cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.binary.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "SNAPPY"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap cache enabled with ZSTD cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.data.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "ZSTD"),
    new BenchmarkConfig()
      .setBenchmarkConfName("parquet w/o index oap binary cache enabled with ZSTD cache compress")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
      .setSparkConf("spark.sql.oap.parquet.binary.cache.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compress.enable", "true")
      .setSparkConf("spark.sql.oap.data.fiber.cache.compression.codec", "ZSTD")
  )
}

//trait OapStrategyConfigSet extends BenchmarkConfigSelector{
//  // TODO: choose conf
//  def allConfigurations: Seq[BenchmarkConfig] = Seq(
//    new BenchmarkConfig()
//      .setBenchmarkConfName("oapStrategy enabled")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
//      .setSparkConf("spark.sql.oap.oindex.eis.enabled", "false")
//      .setSparkConf("spark.sql.oap.strategies.enabled", "true"),
//    new BenchmarkConfig()
//      .setBenchmarkConfName("oapStrategy disabled")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
//      .setSparkConf("spark.sql.oap.oindex.eis.enabled", "true")
//      .setSparkConf("spark.sql.oap.strategies.enabled", "false")
//  )
//}
//
//trait CacheMissConfigSet extends BenchmarkConfigSelector {
//  def allConfigurations: Seq[BenchmarkConfig] = Seq(
//    new BenchmarkConfig()
//      .setBenchmarkConfName("offheap memory only")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setSparkConf("spark.memory.offHeap.enabled", "true")
//      .setSparkConf("spark.memory.offHeap.size", "10g")
//      .setSparkConf("spark.sql.oap.oindex.eis.enabled", "false"),
//    new BenchmarkConfig()
//      .setBenchmarkConfName("no offheap used")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setSparkConf("spark.memory.offHeap.enabled", "false")
//      .setSparkConf("spark.sql.oap.oindex.eis.enabled", "false")
//  )
//}
//
trait LocalClusterConfigSet extends BenchmarkConfigSelector {
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig] = Seq(
    new BenchmarkConfig()
      .setBenchmarkConfName("local cluster 100m offheap")
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSparkConf("spark.memory.offHeap.enabled", "true")
      .setSparkConf("spark.memory.offHeap.size", "100m")
//    // TODO: Here this config does not work because in local
    // mode, MemoryManager initialization do only once as it
    // is a object. 
//    new BenchmarkConfig()
//      .setBenchmarkConfName("executor on/off heap: 100/0")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
//      .setSparkConf("spark.memory.offHeap.enabled", "true")
//      .setSparkConf("spark.yarn.executor.memoryOverhead", "1g")
//      .setSparkConf("spark.executor.memory", "100g")
//      .setSparkConf("spark.sql.oap.offheap.enable", "false")
//    ,
//    new BenchmarkConfig()
//      .setBenchmarkConfName("executor on/off heap: 100+10/0")
//      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
//      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
//      .setSparkConf("spark.memory.offHeap.enabled", "true")
//      .setSparkConf("spark.yarn.executor.memoryOverhead", "1g")
//      .setSparkConf("spark.executor.memory", "110g")
//      .setSparkConf("spark.sql.oap.onheap.size", "10g")
//      .setSparkConf("spark.sql.oap.offheap.enable", "false")
  )
}

