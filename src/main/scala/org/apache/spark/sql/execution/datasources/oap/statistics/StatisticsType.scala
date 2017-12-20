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

package org.apache.spark.sql.execution.datasources.oap.statistics

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.types.StructType

private[oap] object StatisticsType {
  val TYPE_MIN_MAX: Int = 0
  val TYPE_SAMPLE_BASE: Int = 1
  val TYPE_PART_BY_VALUE: Int = 2
  val TYPE_BLOOM_FILTER: Int = 3

  def unapply(t: Int): Option[StructType => StatisticsReader] = t match {
    case TYPE_MIN_MAX => Some(new MinMaxStatisticsReader(_))
    case TYPE_SAMPLE_BASE => Some(new SampleBasedStatisticsReader(_))
    case TYPE_PART_BY_VALUE => Some(new PartByValueStatisticsReader(_))
    case TYPE_BLOOM_FILTER => Some(new BloomFilterStatisticsReader(_))
    case _ => None
  }

  def unapply(name: String): Option[(StructType, Configuration) => StatisticsWriter] = name match {
    case "MINMAX" =>
      Some((schema: StructType, conf: Configuration) => new MinMaxStatisticsWriter(schema, conf))
    case "SAMPLE" =>
      Some((schema: StructType, conf: Configuration) =>
        new SampleBasedStatisticsWriter(schema, conf))
    case "PARTBYVALUE" =>
      Some((schema: StructType, conf: Configuration) =>
        new PartByValueStatisticsWriter(schema, conf))
    case "BLOOM" =>
      Some((schema: StructType, conf: Configuration) =>
        new BloomFilterStatisticsWriter(schema, conf))
    case _ => None
  }
}
