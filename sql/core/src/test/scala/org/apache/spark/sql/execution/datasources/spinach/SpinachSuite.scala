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

package org.apache.spark.sql.execution.datasources.spinach

import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

class SpinachSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._
  private var path: File = null
  private var df: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    path = Utils.createTempDir()
    path.delete()

    val df = sparkContext.parallelize(1 to 100, 3)
      .map(i => (i, i + 100, s"this is row $i"))
      .toDF("a", "b", "c")

    df.write.format("spn").mode(SaveMode.Overwrite).save(path.getAbsolutePath)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  test("reading spinach file") {
    verifyFrame(sqlContext.read.format("spn").load(path.getAbsolutePath))
  }

  /** Verifies data and schema. */
  private def verifyFrame(df: DataFrame): Unit = {
    // schema
    assert(df.schema == new StructType()
      .add("a", IntegerType).add("b", IntegerType).add("c", StringType))

    // verify content
    val data = df.collect().sortBy(_.getInt(0)) // sort locally
    assert(data.length == 100)
    assert(data(0) == Row(1, 101, "this is row 1"))
    assert(data(1) == Row(2, 102, "this is row 2"))
    assert(data(99) == Row(100, 200, "this is row 100"))
  }
}
