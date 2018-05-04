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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}

class DropIndexCommandSuite extends SharedOapContext with BeforeAndAfterEach {

  test("drop index on empty table") {
    Seq("oap", "parquet").foreach(format =>
      withTable("empty_table") {
        sql(
          s"""CREATE TABLE empty_table (a int, b int)
             | USING $format""".stripMargin)
        sql(s"""DROP OINDEX index_a ON empty_table""")
      }
    )
  }

  test("drop index on empty table after create index") {
    Seq("oap", "parquet").foreach(format =>
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
}
