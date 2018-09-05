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

package org.apache.spark.sql.execution.datasources.oap.utils

import javax.annotation.concurrent.NotThreadSafe

import scala.collection.mutable
import scala.xml.XML

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.util.Utils

/**
 * A xml parser used for parse persistent memory config.
 */
@NotThreadSafe
object PersistentConfigUtils extends Logging {
  type PMProperty = (String, Long, Long)
  private val DEFAULT_PERSISTENT_MEMORY_CONFIG_FILE = "persistent-memory.xml"
  private val NUMA_NODE_PROPERTY = "numanode"
  private val NUAM_NODE_ID_PROPERTY = "@id"
  private val INITIAL_PATH_PROPERTY = "initialPath"
  private val INITIAL_SIZE_PROPERTY = "initialSize"
  private val RESERVED_SIZE_PROPERTY = "reservedSize"
  private val numaToPMProperty = new mutable.HashMap[Int, PMProperty]()

  def parseConfig(conf: SparkConf): mutable.HashMap[Int, PMProperty] = {
    val configFile = conf.get(OapConf.OAP_FIBERCACHE_PERSISTENT_MEMORY_CONFIG_FILE.key,
      DEFAULT_PERSISTENT_MEMORY_CONFIG_FILE)
    // If already parsed, just return it
    if (numaToPMProperty.size == 0) {
      val is = Utils.getSparkClassLoader.getResourceAsStream(configFile)
      if (is == null) {
        throw new RuntimeException("Intel Optane DC persistent memory configuration file not " +
          "found. Please provide it.")
      } else {
        logInfo(s"Parse Intel Optane DC persistent memory configuration file from ${configFile}.")
      }

      val xml = XML.load(is)
      for (numaNode <- (xml \\ NUMA_NODE_PROPERTY)) {
        val numaNodeId = (numaNode \ NUAM_NODE_ID_PROPERTY).text.trim.toInt
        val initialPath = (numaNode \ INITIAL_PATH_PROPERTY).text.trim
        val initialSizeStr = (numaNode \ INITIAL_SIZE_PROPERTY).text.trim
        val initialSize = Utils.byteStringAsBytes(initialSizeStr)
        val reservedSizeStr = (numaNode \ RESERVED_SIZE_PROPERTY).text.trim
        val reservedSize = Utils.byteStringAsBytes(reservedSizeStr)
        numaToPMProperty += ((numaNodeId, (initialPath, initialSize, reservedSize)))
      }
    }

    numaToPMProperty
  }

  /**
   * Get the number of numa node.
   */
  def totalNumaNode(conf: SparkConf): Int = {
    if (numaToPMProperty.isEmpty) {
      parseConfig(conf)
      require(numaToPMProperty.nonEmpty, "never should arrive here.")
    }

    numaToPMProperty.size
  }
}
