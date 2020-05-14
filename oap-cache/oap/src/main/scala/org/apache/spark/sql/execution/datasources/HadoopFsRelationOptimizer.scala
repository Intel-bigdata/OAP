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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.oap.{OapFileFormat, OptimizedOrcFileFormat, OptimizedParquetFileFormat}
import org.apache.spark.sql.execution.datasources.orc.ReadOnlyNativeOrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ReadOnlyParquetFileFormat}
import org.apache.spark.sql.hive.orc.ReadOnlyOrcFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types.{AtomicType, StructType}

object HadoopFsRelationOptimizer extends Logging {

  /**
   * Return (HadoopFsRelation, Boolean) Tuple,
   * if use Oap, return (OptimizedRelation, true)
   * else (OriginalRelation, false).
   */
  def tryOptimize(
      relation: HadoopFsRelation,
      partitionKeyFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      outputSchema: StructType): (HadoopFsRelation, Boolean) = {

    def selectedPartitions: Seq[PartitionDirectory] =
      relation.location.listFiles(partitionKeyFilters, Nil)

    relation.fileFormat match {
      case _: ReadOnlyParquetFileFormat =>
        logInfo("index operation for parquet, retain ReadOnlyParquetFileFormat.")
        (relation, false)
      case _: ReadOnlyOrcFileFormat | _: ReadOnlyNativeOrcFileFormat =>
        logInfo("index operation for orc, retain ReadOnlyOrcFileFormat.")
        (relation, false)
      // There are two scenarios will use OptimizedParquetFileFormat:
      // 1. canUseCache: OAP_PARQUET_ENABLED is true and OAP_PARQUET_DATA_CACHE_ENABLED is true
      //    and PARQUET_VECTORIZED_READER_ENABLED is true and WHOLESTAGE_CODEGEN_ENABLED is
      //    true and all fields in outputSchema are AtomicType.
      // 2. canUseIndex: OAP_PARQUET_ENABLED is true and hasAvailableIndex.
      // Other scenarios still use ParquetFileFormat.
      case _: ParquetFileFormat
        if relation.sparkSession.conf.get(OapConf.OAP_PARQUET_ENABLED) =>

        val optimizedParquetFileFormat = new OptimizedParquetFileFormat
        optimizedParquetFileFormat
          .init(relation.sparkSession,
            relation.options,
            selectedPartitions.flatMap(p => p.files))

        def checkParquetDataCacheConfig(): Unit = {
          val runtimeConf = relation.sparkSession.conf
          val binaryCacheEnabled = runtimeConf.get(OapConf.OAP_PARQUET_BINARY_DATA_CACHE_ENABLED)
          val vectorCacheEnabled = runtimeConf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED)
          assert(!(binaryCacheEnabled && vectorCacheEnabled),
            "Current version cannot enabled both binary Cache and vector Cache")
        }

        def canUseBinaryCache: Boolean = {
          val runtimeConf = relation.sparkSession.conf
          val ret = runtimeConf.get(OapConf.OAP_PARQUET_BINARY_DATA_CACHE_ENABLED)
          logDebug(s"config - ${OapConf.OAP_PARQUET_BINARY_DATA_CACHE_ENABLED.key} is $ret")
          if (ret) {
            logInfo("binary data cache enable and suitable for use , " +
              "will replace with OptimizedParquetFileFormat.")
          }
          ret
        }

        def canUseVectorCache: Boolean = {
          val runtimeConf = relation.sparkSession.conf
          val cacheEnabled = runtimeConf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED)
          logDebug(s"config - ${OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key} is $cacheEnabled")
          val ret = cacheEnabled && runtimeConf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED) &&
            runtimeConf.get(SQLConf.WHOLESTAGE_CODEGEN_ENABLED) &&
            outputSchema.forall(_.dataType.isInstanceOf[AtomicType])
          if (ret) {
            logInfo("vector data cache enable and suitable for use , " +
              "will replace with OptimizedParquetFileFormat.")
          }
          ret
        }

        def canUseIndex: Boolean = {
          val indexEnabled = relation.sparkSession.conf.get(OapConf.OAP_PARQUET_INDEX_ENABLED)
          logDebug(s"config - ${OapConf.OAP_PARQUET_INDEX_ENABLED.key} is $indexEnabled")
          val ret = indexEnabled && optimizedParquetFileFormat.hasAvailableIndex(dataFilters)
          if (ret) {
            logInfo("index enable and hasAvailableIndex is true, " +
              "will replace with OptimizedParquetFileFormat.")
          }
          ret
        }

        checkParquetDataCacheConfig()
        if (canUseBinaryCache || canUseVectorCache || canUseIndex) {
          (relation.copy(fileFormat = optimizedParquetFileFormat)(relation.sparkSession), true)
        } else {
          logInfo("neither index nor data cache is available, retain ParquetFileFormat.")
          (relation, false)
        }

      case a if relation.sparkSession.conf.get(OapConf.OAP_ORC_ENABLED) &&
        (a.isInstanceOf[org.apache.spark.sql.hive.orc.OrcFileFormat] ||
          a.isInstanceOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat]) =>
        val optimizedOrcFileFormat = new OptimizedOrcFileFormat
        optimizedOrcFileFormat
          .init(relation.sparkSession,
            relation.options,
            selectedPartitions.flatMap(p => p.files))

        def canUseCache: Boolean = {
          val runtimeConf = relation.sparkSession.conf
          var vectorCacheEnabled = runtimeConf.get(OapConf.OAP_ORC_DATA_CACHE_ENABLED)
          logDebug(s"config - ${OapConf.OAP_ORC_DATA_CACHE_ENABLED.key} is $vectorCacheEnabled")
          vectorCacheEnabled = vectorCacheEnabled &&
            runtimeConf.get(SQLConf.ORC_VECTORIZED_READER_ENABLED) &&
            runtimeConf.get(SQLConf.WHOLESTAGE_CODEGEN_ENABLED) &&
            // runtimeConf.get(SQLConf.ORC_COPY_BATCH_TO_SPARK) &&
            outputSchema.forall(_.dataType.isInstanceOf[AtomicType])
          val binaryCacheEnabled = runtimeConf.get(OapConf.OAP_ORC_BINARY_DATA_CACHE_ENABLED)
          logDebug(s"config - ${OapConf.OAP_ORC_BINARY_DATA_CACHE_ENABLED.key}" +
            s"is $binaryCacheEnabled")
          val ret = vectorCacheEnabled || binaryCacheEnabled
          if (ret) {
            logInfo("data cache enable and suitable for use , " +
              "will replace with optimizedOrcFileFormat.")
          }
          ret
        }


        if (optimizedOrcFileFormat.hasAvailableIndex(dataFilters) || canUseCache) {
          logInfo("hasAvailableIndex or enable cache, will replace with optimizedOrcFileFormat.")
          val orcOptions: Map[String, String] =
            Map(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key ->
              relation.sparkSession.sessionState.conf.orcFilterPushDown.toString) ++
              relation.options

          (relation.copy(fileFormat = optimizedOrcFileFormat,
            options = orcOptions)(relation.sparkSession), true)
        } else {
          logInfo("hasAvailableIndex = false, will retain OrcFileFormat.")
          (relation, false)
        }

      case _: OapFileFormat =>
        relation.fileFormat.asInstanceOf[OapFileFormat].init(
          relation.sparkSession,
          relation.options,
          selectedPartitions.flatMap(p => p.files))
        (relation, false)

      case _: FileFormat =>
        (relation, false)
    }
  }
}
