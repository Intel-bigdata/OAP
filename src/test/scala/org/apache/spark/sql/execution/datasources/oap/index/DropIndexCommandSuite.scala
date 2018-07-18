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

package org.apache.spark.sql.execution.datasources.oap.index

import org.apache.hadoop.fs.{Path, PathFilter}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.DebugFilesystem
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}

class DropIndexCommandSuite extends SharedOapContext with BeforeAndAfterEach {

  import testImplicits._

  private val tableFormats = Seq("oap", "parquet")

  // afterEach don't assertNoOpenStreams because OapDataFile has no method to close meta cache.
  protected override def afterEach(): Unit = DebugFilesystem.clearOpenStreams()

  test("drop index on empty table") {
    tableFormats.foreach(format =>
      withTable("empty_table") {
        sql(
          s"""CREATE TABLE empty_table (a int, b int)
             | USING $format""".stripMargin)
        sql(s"""DROP OINDEX index_a ON empty_table""")
      }
    )
  }

  test("drop index on empty table after create index") {
    tableFormats.foreach(format =>
      withTable("empty_table") {
        sql(
          s"""CREATE TABLE empty_table (a int, b int)
             | USING $format""".stripMargin)
        withIndex(TestIndex("empty_table", "a_index")) {
          sql("CREATE OINDEX a_index ON empty_table(a)")
        }
      }
    )
  }

  test("test refresh on different partition and drop index") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")

    withFileSystem { fs => {
      tableFormats.foreach(format => {
        withTempDir { dir =>
          val pathString = dir.getAbsolutePath
          val basePath = new Path(pathString)

          withTable("partitioned_table") {
            sql(
              s"""CREATE TABLE partitioned_table (a int, b int)
                 | USING $format
                 | OPTIONS (path '$pathString')
                 | PARTITIONED by (b)""".stripMargin)
            withIndex(TestIndex("partitioned_table", "a_index")) {
              sql(
                """
                  |INSERT OVERWRITE TABLE partitioned_table
                  |partition (b=1)
                  |SELECT key from t where value < 4
                """.stripMargin)

              sql("CREATE OINDEX a_index ON partitioned_table(a)")

              // after create, record index file count
              val bEq1IndexCount = fs.listStatus(new Path(basePath, "b=1"), new PathFilter {
                override def accept(path: Path): Boolean = path.getName.endsWith(".a_index.index")
              }).length

              sql(
                """
                  |INSERT INTO TABLE partitioned_table
                  |partition (b=2)
                  |SELECT key from t where value == 4
                """.stripMargin)

              sql(
                """
                  |INSERT INTO TABLE partitioned_table
                  |partition (b=1)
                  |SELECT key from t where value == 5
                """.stripMargin)

              sql("REFRESH OINDEX ON partitioned_table")

              // after refresh, record index file count
              val bEq1IndexCountNew = fs.listStatus(new Path(basePath, "b=1"), new PathFilter {
                override def accept(path: Path): Boolean = path.getName.endsWith(".a_index.index")
              }).length
              val bEq2IndexCount = fs.listStatus(new Path(basePath, "b=2"), new PathFilter {
                override def accept(path: Path): Boolean = path.getName.endsWith(".a_index.index")
              }).length

              // assert our expectations
              assert(bEq1IndexCountNew > bEq1IndexCount)
              assert(bEq2IndexCount > 0)
            }

            // after drop, record index file count
            val bEq1IndexCount = fs.listStatus(new Path(basePath, "b=1"), new PathFilter {
              override def accept(path: Path): Boolean = path.getName.endsWith(".a_index.index")
            }).length
            val bEq2IndexCount = fs.listStatus(new Path(basePath, "b=2"), new PathFilter {
              override def accept(path: Path): Boolean = path.getName.endsWith(".a_index.index")
            }).length

            // assert our expectations
            assert(bEq1IndexCount == 0)
            assert(bEq2IndexCount == 0)
          }
        }
      })
    }}
  }
}
