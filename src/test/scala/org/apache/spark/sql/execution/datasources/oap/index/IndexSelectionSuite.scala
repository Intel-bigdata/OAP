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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils


class IndexSelectionSuite extends SharedOapContext with BeforeAndAfterEach{

  import testImplicits._
  private var tempDir: File = _
  private var path: Path = _

  override def beforeEach(): Unit = {
    val data = (1 to 300).map(i => (i, i + 100, i + 200, i + 300, s"this is row $i"))
    val df = sparkContext.parallelize(data, 3).toDF("a", "b", "c", "d", "e")
    df.write.format("oap").mode(SaveMode.Overwrite).save(tempDir.getAbsolutePath)
    val oapDf = sqlContext.read.format("oap").load(tempDir.getAbsolutePath)
    oapDf.createOrReplaceTempView("oap_test")
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Utils.createTempDir()
    path = new Path(
      new File(tempDir.getAbsolutePath, OapFileFormat.OAP_META_FILE).getAbsolutePath)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  private def assertIndexer(ic: IndexContext, attrNum: Int,
                            targetIdxName: String, targetLastIdx: Int,
                            maxChooseSize: Int = 1): Unit = {
    val availableIndexers = ic.getAvailableIndexers(attrNum, maxChooseSize)
    val lastIdx = availableIndexers.headOption match {
      case Some(v) => v._1
      case _ => -1
    }
    val idxMeta = availableIndexers.headOption match {
      case Some(v) => v._2
      case _ => null
    }
    assert(lastIdx == targetLastIdx)
    if (idxMeta != null) {
      assert(idxMeta.name equals targetIdxName)
    }
  }

  test("Non-Index Test") {
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(
      Or(And(GreaterThan("a", 9), LessThan("a", 14)),
        And(GreaterThan("a", 23), LessThan("a", 166)))
    )
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 1, "", -1)
  }

  test("Single Column Index Test") {
    sql("create oindex idxa on oap_test(a)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(
      Or(And(GreaterThan("a", 9), LessThan("a", 14)),
        And(GreaterThan("a", 23), LessThan("a", 166)))
    )
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 1, "idxa", 0)
  }

  test("Multiple Indexes Test1") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(
      Or(And(GreaterThan("b", 9), LessThan("b", 14)),
        And(GreaterThan("b", 13), LessThan("b", 16)))
    )
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 1, "idxb", 0)
  }

  test("Multiple Indexes Test2") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")
    sql("create oindex idxe on oap_test(e)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(EqualTo("e", "this is row 3"))
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 1, "idxe", 0)
  }

  test("Multiple Indexes Test3") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")
    sql("create oindex idxe on oap_test(e)")
    sql("create oindex idxABC on oap_test(a, b, c)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(
      Or(And(GreaterThan("a", 9), LessThan("a", 14)),
        And(GreaterThan("a", 23), LessThan("a", 166)))
    )
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 1, "idxa", 0)
  }

  test("Multiple Indexes Test4") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")
    sql("create oindex idxe on oap_test(e)")
    sql("create oindex idxABC on oap_test(a, b, c)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(EqualTo("a", 8), GreaterThanOrEqual("b", 10))
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 2, "idxABC", 1)

    ic.clear()
    val filters2: Array[Filter] = Array(
      EqualTo("a", 8), GreaterThanOrEqual("b", 10), EqualTo("c", 3))
    ScannerBuilder.build(filters2, ic)
    assertIndexer(ic, 3, "idxABC", 1)

    ic.clear()
    val filters3: Array[Filter] = Array(
      EqualTo("a", 8), EqualTo("b", 10), EqualTo("c", 3))
    ScannerBuilder.build(filters3, ic)
    assertIndexer(ic, 3, "idxABC", 2)

    ic.clear()
    val filters4: Array[Filter] = Array(
      EqualTo("a", 8), EqualTo("b", 10), LessThanOrEqual("c", 3))
    ScannerBuilder.build(filters4, ic)
    assertIndexer(ic, 3, "idxABC", 2)

    ic.clear()
    val filters5: Array[Filter] = Array(
      EqualTo("a", 8), EqualTo("b", 10), Or(EqualTo("c", 3), GreaterThan("c", 90)))
    ScannerBuilder.build(filters5, ic)
    assertIndexer(ic, 3, "idxABC", 2)

    ic.clear()
    val filters6: Array[Filter] = Array(
      GreaterThan("a", 8), LessThan("a", 20),
      EqualTo("b", 10), Or(EqualTo("c", 3), GreaterThan("c", 90)))
    ScannerBuilder.build(filters6, ic)
    assertIndexer(ic, 3, "idxa", 0)
  }

  test("Multiple Indexes Test5") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")
    sql("create oindex idxe on oap_test(e)")
    sql("create oindex idxABC on oap_test(a, b, c)")
    sql("create oindex idxACD on oap_test(a, c, d)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(
      EqualTo("a", 8), GreaterThanOrEqual("b", 10), EqualTo("c", 3))
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 3, "idxABC", 1)
  }

  test("Multiple Indexes Test6") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")
    sql("create oindex idxe on oap_test(e)")
    sql("create oindex idxABC on oap_test(a, b, c)")
    sql("create oindex idxACD on oap_test(a, c, d)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(
      EqualTo("a", 8), GreaterThanOrEqual("d", 10), EqualTo("c", 3))
    ScannerBuilder.build(filters, ic)
    assertIndexer(ic, 3, "idxACD", 2)
  }

  test("Bitmap Scanner only supports equal query.") {
    sql("create oindex idxa on oap_test(a) USING BITMAP")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val nonEqualFilters: Array[Filter] = Array(
      Or(And(GreaterThan("a", 9), LessThan("a", 14)),
        And(GreaterThan("a", 23), LessThan("a", 166)))
    )
    ScannerBuilder.build(nonEqualFilters, ic)
    // Non equal filters won't build BitmapScanner.
    assert(ic.getScanners.isEmpty)
    val equalFilters: Array[Filter] = Array(Or(EqualTo("a", 14), EqualTo("b", 166)))
    ScannerBuilder.build(equalFilters, ic)
    assert(ic.getScanners.get.scanners.head.isInstanceOf[BitMapScanner])
  }

  test("Multiple Indexes Test7") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")
    sql("create oindex idxe on oap_test(e)")
    sql("create oindex idxABC on oap_test(a, b, c)")
    sql("create oindex idxACD on oap_test(a, c, d)")
    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(
      EqualTo("a", 8), GreaterThanOrEqual("d", 10), EqualTo("e", 3))
    ScannerBuilder.build(filters, ic)
    val expectIdxs: ArrayBuffer[(Int, String)] = ArrayBuffer()
    expectIdxs += ((0, "idxa"), (0, "idxe"))
    val availableIndexers = ic.getAvailableIndexers(3, 2)
    val availIdxs = new ArrayBuffer[(Int, String)]()
    for (indexer <- availableIndexers) {
      availIdxs.append((indexer._1, indexer._2.name))
    }
    assert(availIdxs sameElements expectIdxs)
  }

  test("Allow to disable specific indices") {
    sql("create oindex idxa on oap_test(a)")
    sql("create oindex idxb on oap_test(b)")

    val oapMeta = DataSourceMeta.initialize(path, configuration)
    val ic = new IndexContext(oapMeta)
    val filters: Array[Filter] = Array(EqualTo("a", 8), EqualTo("b", 9))
    ScannerBuilder.build(filters, ic, Map.empty, 2)
    assert(ic.getScanners.get.scanners.length == 2)
    ic.clear()

    ScannerBuilder.build(filters, ic, Map.empty, 1, "idxa, idxb")
    assert(ic.getScanners.isEmpty)
  }
}
