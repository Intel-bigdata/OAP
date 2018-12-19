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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.parquet.column.Dictionary
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, TestFiberCache}
import org.apache.spark.sql.test.oap.SharedOapContext

abstract class DataFiberReaderWriterSuite extends SparkFunSuite with SharedOapContext
  with BeforeAndAfterEach with Logging {

  protected val total: Int = 10000
  protected val start: Int = 4096
  protected val num: Int = 4096
  protected val ints: Array[Int] = Array[Int](1, 667, 9999)
  protected val rowIdList = {
    val ret = new IntArrayList(3)
    ints.foreach(ret.add)
    ret
  }

  protected var fiberCache: FiberCache = _

  protected override def afterEach(): Unit = {
    if (fiberCache !== null) {
      new TestFiberCache(fiberCache).free()
      fiberCache = null
    }
  }

  protected def dictionary: Dictionary

}
