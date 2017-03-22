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

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

private [spinach] object BPlusTreeMultiColumnSearchSuite extends Serializable {
//  implicit def transformToInternalRow(array: Array[Seq[Any]]): Array[Key]
//  = array.map(InternalRow.fromSeq)
  implicit def transform(values: Seq[Any]): Key = InternalRow.fromSeq(values)

  val indexMeta: IndexMeta = new IndexMeta("idxABC",
    BTreeIndex(Seq(BTreeIndexEntry(0), BTreeIndexEntry(1), BTreeIndexEntry(2)))) {

    // B+ Tree structure:
    // [(3, 4, 5.7)       (3, 6, 1.0)           (8, 1, 5.95)    (16, 5, 0)] <----- Keys In Root Node
    //      |                   |                    |                |
    // [(3,4,5.7),          [(3,6,1.0), ->      [(8,1,5.95),       [(16,5,0.0) <--- Second Level Key
    //   | (3,4,6.3),  -->    | (3,6,2.4),  -->   | (8,10,3.4),  -->    | (16,5,20.9),
    //   |  | (3,4,10.0),     |  |(8,1,0.5),      |   | (16,2,9.7),     |   | (16,5,30.0),
    //   |  |  | (3,4,12.3),  |  |   | (8,1,1.4)] |   |   |(16,5,-3.7)] |   |   | (16,5,35.3)]
    //   |  |  |  |(3,5,1.0)] |  |   |    |       |   |   |    |        |   |   |   |
    //   |  |  |  |    |      |  |   |    |       |   |   |    |        |   |   |   |
    //  30  40 50 52   80     90 100 130 150     160  170 180 185      193 200 214  223 <--- Values
    //  31  41    55   81     91 101 131         161  171     187              218
    //  32  42    60   82        102 132         162

    def node4 = new LeafNode(
      Array(Seq[Any](16, 5, 0.0), Seq[Any](16, 5, 20.9),
        Seq[Any](16, 5, 30.0), Seq[Any](16, 5, 35.3)),
      Array(new IntValues(Array(193)), new IntValues(Array(200)),
        new IntValues(Array(214, 218)), new IntValues(Array(223))), null)

    def node3 = new LeafNode(
      Array(Seq[Any](8, 1, 5.95), Seq[Any](8, 10, 3.4),
        Seq[Any](16, 2, 9.7), Seq[Any](16, 5, -3.7) ),
      Array(new IntValues(Array(160, 161, 162)), new IntValues(Array(170, 171)),
        new IntValues(Array(180)), new IntValues(Array(185, 187))), node4)

    def node2 = new LeafNode(
      Array(Seq[Any](3, 6, 1.0), Seq[Any](3, 6, 2.4), Seq[Any](8, 1, 0.5), Seq[Any](8, 1, 1.4)),
      Array(new IntValues(Array(90, 91)), new IntValues(Array(100, 101, 102)),
        new IntValues(Array(130, 131, 132)), new IntValues(Array(150))), node3)

    def node1 = new LeafNode(
      Array(Seq[Any](3, 4, 5.7), Seq[Any](3, 4, 6.3), Seq[Any](3, 4, 10.0),
        Seq[Any](3, 4, 12.3), Seq[Any](3, 5, 1.0) ),
      Array(new IntValues(Array(30, 31, 32)), new IntValues(Array(40, 41, 42)),
        new IntValues(Array(50)), new IntValues(Array(52, 55, 60)),
        new IntValues(Array(80, 81, 82))), node2)

    def root = new NonLeafNode(
      Array(Seq[Any](3, 4, 5.7), Seq[Any](3, 6, 1.0), Seq[Any](8, 1, 5.95), Seq[Any](16, 5, 0.0)),
      Array(node1, node2, node3, node4))

    override def open(data: IndexFiberCacheData, schema: StructType): IndexNode = root
  }
}

private[spinach] class BPlusTreeMultiColumnSearchSuite
  extends SparkFunSuite with Logging with BeforeAndAfterAll {
  val conf: Configuration = new Configuration()

  val meta = new DataSourceMeta(
    null, Array(BPlusTreeMultiColumnSearchSuite.indexMeta),
    new StructType().add("a", IntegerType, true).add("b", IntegerType, true)
      .add("c", DoubleType, true).add("d", StringType, true),
    SpinachFileFormat.SPINACH_DATA_FILE_CLASSNAME, null)

  private def assertScanner(meta: DataSourceMeta,
                             filters: Array[Filter],
                             expectedUnHandleredFilter: Array[Filter],
                             expectedIds: Set[Int]): Unit = {
    val ic = new IndexContext(meta)
    val unHandledFilters = ScannerBuilder.build(filters, ic)
    assert(unHandledFilters.sameElements(expectedUnHandleredFilter))
    ic.getScanner match {
      case Some(scanner: BPlusTreeScanner) =>
        assert(scanner._init(
          BPlusTreeMultiColumnSearchSuite.indexMeta.open(null, null)).toSet === expectedIds, "")
      case None => throw new Exception(s"expect scanner, but got None")
    }
  }

  test("a>=8 & a<=9") {
    val filters: Array[Filter] = Array(LessThanOrEqual("a", 9), GreaterThanOrEqual("a", 8))
    assertScanner(meta, filters, Array(), Set(130, 131, 132, 150, 160, 161, 162, 170, 171))
  }

  test("a=8") {
    val filters: Array[Filter] = Array(EqualTo("a", 8))
    assertScanner(meta, filters, Array(), Set(130, 131, 132, 150, 160, 161, 162, 170, 171))
  }

  test("a<3") {
    val filters: Array[Filter] = Array(LessThan("a", 3))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a<=3") {
    val filters: Array[Filter] = Array(LessThanOrEqual("a", 3))
    assertScanner(meta, filters, Array(), Set(30, 31, 32, 40, 41, 42, 50, 52, 55, 60,
      80, 81, 82, 90, 91, 100, 101, 102))
  }

  test("a=8 & b=1") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), EqualTo("b", 1))
    assertScanner(meta, filters, Array(), Set(130, 131, 132, 150, 160, 161, 162))
  }

  test("a=16 & b=2") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 2))
    assertScanner(meta, filters, Array(), Set(180))
  }

  test("a=8 & b=9") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), EqualTo("b", 9))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=8 & b<9") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), LessThan("b", 9))
    assertScanner(meta, filters, Array(), Set(130, 131, 132, 150, 160, 161, 162))
  }

  test("a=8 & b>9") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), GreaterThan("b", 9))
    assertScanner(meta, filters, Array(), Set(170, 171))
  }

  test("a=8 & b>=9") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), GreaterThan("b", 9))
    assertScanner(meta, filters, Array(), Set(170, 171))
  }

  test("a=8 & b>=10") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), GreaterThanOrEqual("b", 10))
    assertScanner(meta, filters, Array(), Set(170, 171))
  }

  test("a=8 & b>10") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), GreaterThan("b", 10))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=8 & b=1 & c=1.4") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), EqualTo("b", 1), EqualTo("c", 1.4))
    assertScanner(meta, filters, Array(), Set(150))
  }

  test("a=3 & b=6 & c=1") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), EqualTo("b", 6), EqualTo("c", 1.0))
    assertScanner(meta, filters, Array(), Set(90, 91))
  }

  test("a=16 & b=3 & c>10.0") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 3), GreaterThan("c", 10.0))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=3 & 4<b<6") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), LessThan("b", 6), GreaterThan("b", 4))
    assertScanner(meta, filters, Array(), Set(80, 81, 82))
  }

  test("a=3 & 4<b<=6") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), LessThanOrEqual("b", 6),
      GreaterThan("b", 4))
    assertScanner(meta, filters, Array(), Set(80, 81, 82, 90, 91, 100, 101, 102))
  }

  test("a=3 & 4<b<7") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), LessThan("b", 7),
      GreaterThan("b", 4))
    assertScanner(meta, filters, Array(), Set(80, 81, 82, 90, 91, 100, 101, 102))
  }

  test("a=3 & 4<=b<=6") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), LessThanOrEqual("b", 6),
      GreaterThanOrEqual("b", 4))
    assertScanner(meta, filters, Array(), Set(30, 31, 32, 40, 41, 42, 50, 52, 55, 60,
      80, 81, 82, 90, 91, 100, 101, 102))
  }

  test("a=3 & 4<=b<6") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), LessThan("b", 6),
      GreaterThanOrEqual("b", 4))
    assertScanner(meta, filters, Array(), Set(30, 31, 32, 40, 41, 42, 50, 52, 55, 60,
      80, 81, 82))
  }

  test("a=8 & 1<b<3") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), LessThan("b", 3),
      GreaterThan("b", 1))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=8 & 1<b<3 & b>6 and c=14.3") {
    val filters: Array[Filter] = Array(EqualTo("a", 8), LessThan("b", 3),
      GreaterThan("b", 1), GreaterThan("b", 6), EqualTo("c", 14.3))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=16 & b=5 & c>3") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5), GreaterThan("c", 3.0))
    assertScanner(meta, filters, Array(), Set(200, 214, 218, 223))
  }

  test("a=16 & b=5 & c=3") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5), EqualTo("c", 3.0))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=16 & b=5 & c<3") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5), LessThan("c", 3.0))
    assertScanner(meta, filters, Array(), Set(185, 187, 193))
  }

  test("a=16 & b=5 & c>20.9") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5), GreaterThan("c", 20.9))
    assertScanner(meta, filters, Array(), Set(214, 218, 223))
  }

  test("a=16 & b=5 & c=20.9") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5), EqualTo("c", 20.9))
    assertScanner(meta, filters, Array(), Set(200))
  }

  test("a=16 & b=5 & c<20.9") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5), LessThan("c", 20.9))
    assertScanner(meta, filters, Array(), Set(185, 187, 193))
  }

  test("a=16 & b=5 & c<=20.9") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5),
      LessThanOrEqual("c", 20.9))
    assertScanner(meta, filters, Array(), Set(185, 187, 193, 200))
  }

  // The correct result is Set(100,101,102), however, this test is only for index utilization.
  // Our approach of Spinach is:
  // the multi-column index will use the first two columns to search, and deliver the result set to
  // upper layer, and then Spark SQL will use Filters of SQL statements to filter out
  // the unmatched keys within this result set
  test("a=3 & b>5 & c>1.0") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), GreaterThan("b", 5),
      GreaterThan("c", 1.0))
    assertScanner(meta, filters, Array(), Set(90, 91, 100, 101, 102))
  }

  // The correct result is Set(170, 171), however, this test is only for index utilization.
  // Our approach of Spinach is:
  // the multi-column index will use the first column to search, and deliver the result set to
  // upper layer, and then Spark SQL will use Filters of SQL statements to filter out
  // the unmatched keys within this result set
  test("3<a<9 & b>5 & c>1.0") {
    val filters: Array[Filter] = Array(GreaterThan("a", 3), GreaterThan("b", 5),
      GreaterThan("c", 1.0), LessThan("a", 9))
    assertScanner(meta, filters, Array(), Set(130, 131, 132, 150, 160, 161, 162, 170, 171))
  }

  test("a=16 & b=5 & c<=20.9 & c>100") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5),
      LessThanOrEqual("c", 20.9), GreaterThan("c", 100.0))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=16 & b=5 & c<=20.9 & c=1") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5),
      LessThanOrEqual("c", 20.9), EqualTo("c", 1.0))
    assertScanner(meta, filters, Array(), Set())
  }

  test("a=16 & b=5 & c<=20.9 & b=10") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5),
      LessThanOrEqual("c", 20.9), EqualTo("b", 10))
    assertScanner(meta, filters, Array(), Set())
  }

  // Multi-column and Multi-range Test
  // Although "Or" is not supported in most relational databases's B+ tree index,
  // Spinach supports "Or" in order to make full use of index, but with one limit:
  // Only when one attribute exists in "Or" predicate is B+ tree index able to support "Or"
  test("a=3 & (b=5 || b=6)") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), Or(EqualTo("b", 5), EqualTo("b", 6)) )
    assertScanner(meta, filters, Array(), Set(80, 81, 82, 90, 91, 100, 101, 102))
  }

  test("a=3 & (b=5 || b=7)") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), Or(EqualTo("b", 5), EqualTo("b", 7)) )
    assertScanner(meta, filters, Array(), Set(80, 81, 82))
  }

  // The correct result is Set(100,101,102), however, this test is only for index utilization.
  // Our approach of Spinach is:
  // the multi-column index will use the first two columns to search, and deliver the result set to
  // upper layer, and then Spark SQL will use Filters of SQL statements to filter out
  // the unmatched keys within this result set
  test("a=3 & (b=5 || b=6) & c=2.4") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), Or(EqualTo("b", 5), EqualTo("b", 6)),
      EqualTo("c", 2.4))
    assertScanner(meta, filters, Array(), Set(80, 81, 82, 90, 91, 100, 101, 102))
  }

  test("a=3 & (b<5 || b>=6)") {
    val filters: Array[Filter] = Array(EqualTo("a", 3), Or(LessThan("b", 5),
      GreaterThanOrEqual("b", 6)) )
    assertScanner(meta, filters, Array(), Set(30, 31, 32, 40, 41, 42, 50, 52,
      55, 60, 90, 91, 100, 101, 102))
  }

  test("a=16 & b=5 &(-5.0<c<1.0 || 25<c<=35.3)") {
    val filters: Array[Filter] = Array(EqualTo("a", 16), EqualTo("b", 5),
      Or(And(GreaterThan("c", -5.0), LessThan("c", 1.0)),
        And(LessThanOrEqual("c", 35.3), GreaterThan("c", 25.0))) )
    assertScanner(meta, filters, Array(), Set(185, 187, 193, 214, 218, 223))
  }

}
