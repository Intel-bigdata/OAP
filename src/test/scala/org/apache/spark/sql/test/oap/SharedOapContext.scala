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

package org.apache.spark.sql.test.oap

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql.{OapExtensions, SparkSession}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.oap.{IndexType, OapFileFormat}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.{OapDriverRuntime, OapRuntime}
import org.apache.spark.sql.test.OapSharedSQLContext

trait SharedOapContext extends SharedOapContextBase {
  protected override def createSparkSession: SparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = SparkSession.builder()
      .master("local[2]")
      .appName("test-oap-context")
      .config(oapSparkConf).getOrCreate()
    OapRuntime.getOrCreate.asInstanceOf[OapDriverRuntime].setTestSession(session)
    session
  }
}

/**
 * Extend this context to test in LocalClusterMode
 */
trait SharedOapLocalClusterContext extends SharedOapContextBase {
  protected override def createSparkSession: SparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = SparkSession.builder()
      .master("local-cluster[2, 2, 1024]")
      .appName("test-oap-local-cluster-context")
      .config(oapSparkConf).getOrCreate()
    OapRuntime.getOrCreate.asInstanceOf[OapDriverRuntime].setTestSession(session)
    session
  }
}

trait SharedOapContextBase extends OapSharedSQLContext {

  // In Spark 2.1, sparkConf is a val. However is Spark 2.2 sparkConf is a function that create
  // a new SparkConf each time.
  val oapSparkConf = sparkConf
  // avoid the overflow of offHeap memory
  oapSparkConf.set("spark.memory.offHeap.size", "100m")
  oapSparkConf.set("spark.sql.extensions", classOf[OapExtensions].getCanonicalName)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sqlContext.setConf(OapConf.OAP_BTREE_ROW_LIST_PART_SIZE, 64)
  }

  // disable file based cbo for all test suite, as it always fails.
  oapSparkConf.set(OapConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.key, "false")

  protected lazy val configuration: Configuration = spark.sessionState.newHadoopConf()

  protected def getOapFileFormat(sparkPlan: SparkPlan): Set[Option[OapFileFormat]] = {
    def getOapFileFormatFromSource(node: SparkPlan): Option[OapFileFormat] = {
      node match {
        case f: FileSourceScanExec =>
          f.relation.fileFormat match {
            case format: OapFileFormat =>
              Some(format)
            case _ => None
          }
        case _ => None
      }
    }

    val ret = new mutable.HashSet[Option[OapFileFormat]]()
    sparkPlan.foreach(node => {
      if (node.isInstanceOf[FilterExec]) {
        node.children.foreach(s => ret.add(getOapFileFormatFromSource(s))
        )
      }
    })
    ret.filter(_.isDefined).toSet
  }

  protected def getColumnsHitIndex(sparkPlan: SparkPlan): Map[String, IndexType] = {
    getOapFileFormat(sparkPlan).map(_1 => _1.map(f => f.getHitIndexColumns))
      .foldLeft(Map.empty[String, IndexType]) { (ret, set) =>
        ret ++ set.getOrElse(Nil)
      }
  }

  /**
   * Drop oindex by `TestIndex` after calling `f`, `TestIndex` use to
   * specify the `tableName`, `indexName` and `partitions`.
   * `partitions` is not necessary unless create index also specify `partitions`.
   */
  protected def withIndex(indices: TestIndex*)(f: => Unit): Unit = {
    try f finally {
      indices.foreach { index =>
        val baseSql = s"DROP OINDEX ${index.indexName} on ${index.tableName}"
        if (index.partitions.isEmpty) {
          spark.sql(baseSql)
        } else {
          val partitionPart = index.partitions.map(p => s"${p.key} = '${p.value}'")
            .mkString(" partition (", ",", ")")
          spark.sql(s"$baseSql $partitionPart")
        }
      }
    }
  }

  /**
   * Creates a FileSystem , which is then passed to `f` and will be closed after `f` returns.
   */
  protected def withFileSystem(f: FileSystem => Unit): Unit = {
    var fs: FileSystem = null
    try {
      fs = FileSystem.get(configuration)
      f(fs)
    } finally {
      if (fs != null) {
        fs.close()
      }
    }
  }
}

case class TestPartition(key: String, value: String)

case class TestIndex(
    tableName: String,
    indexName: String,
    partitions: TestPartition*)
