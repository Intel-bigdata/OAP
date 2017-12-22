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

import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.oap.{IndexType, OapFileFormat}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

trait SharedOapContext extends SharedSQLContext {

  // avoid the overflow of offHeap memory
  sparkConf.set("spark.memory.offHeap.size", "100m")
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sqlContext.setConf(SQLConf.OAP_BTREE_ROW_LIST_PART_SIZE, 64)
  }

  // disable file based cbo for all test suite, as it always fails.
  sparkConf.set(SQLConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.key, "false")

  protected lazy val configuration: Configuration = sparkContext.hadoopConfiguration

  protected implicit def sqlConf: SQLConf = sqlContext.conf

  protected def getColumnsHitIndex(sparkPlan: SparkPlan): Map[String, IndexType] = {
    val ret = new mutable.HashMap[String, IndexType]()
    def getOapFileFormat(node: SparkPlan): Option[OapFileFormat] = {
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

    sparkPlan.foreach(node => {
      if (node.isInstanceOf[FilterExec]) {
        node.children.foreach(s => {
          ret ++= getOapFileFormat(s).map(f => f.getHitIndexColumns).getOrElse(Nil)
        })
      }
    })

    ret.toMap
  }
}
