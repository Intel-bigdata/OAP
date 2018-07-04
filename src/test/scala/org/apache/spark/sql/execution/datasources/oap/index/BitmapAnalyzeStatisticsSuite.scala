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

import org.apache.hadoop.fs.RawLocalFileSystem
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.test.oap.SharedOapContextBase
import org.apache.spark.util.Utils

/*
 * By default, the spark and oap tests are using the same debug file system which
 * will re-open the file for each file read. Therefore the file read will not be
 * impacted even if the input stream is closed already.
 * This test is to mimic the same file system with the customer's QA environment to
 * check and verify that the exception will be thrown as expected if the input stream
 * is closed before the file read when the bimap scanner tries to analyze the statistics.
 */

private[oap] class TestOapSessionWithRawLocalFileSystem(sc: SparkContext)
    extends TestSparkSession(sc) { self =>

  def this(sparkConf: SparkConf) {
    this(new SparkContext(
      "local[2]",
      "test-oap-context",
      sparkConf.set("spark.sql.testkey", "true")
        .set("spark.hadoop.fs.file.impl", classOf[RawLocalFileSystem].getName)))
  }
}

trait SharedOapContextWithRawLocalFileSystem extends SharedOapContextBase {
  protected override def createSparkSession: TestSparkSession = {
    new TestOapSessionWithRawLocalFileSystem(sparkConf)
  }
}

class BitmapAnalyzeStatisticsSuite extends QueryTest with SharedOapContextWithRawLocalFileSystem
    with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    val tempDir = Utils.createTempDir()
    val path = tempDir.getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
            | USING oap
            | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
  }

  test("Bitmap index typical equal test") {
    val data: Seq[(Int, String)] = (1 to 200).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    sql("create oindex idxa on oap_test (a) USING BITMAP")
    checkAnswer(sql(s"SELECT * FROM oap_test WHERE a = 20 OR a = 21"),
      Row(20, "this is test 20") :: Row(21, "this is test 21") :: Nil)
    sql("drop oindex idxa on oap_test")
  }
}
