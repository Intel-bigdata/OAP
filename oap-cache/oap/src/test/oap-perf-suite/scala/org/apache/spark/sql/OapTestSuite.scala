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

/*
 * Base interface for test suite.
 */
package org.apache.spark.sql

import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import sys.process._

abstract class OapTestSuite extends BenchmarkConfigSelector with OapPerfSuiteContext with Logging {

  // Class information
  case class OapBenchmarkTest(_name: String, _sentence: String, _profile: String = "Benchmark") {
    def name = _name
    def sql = _sentence
    def profile = _profile
  }

  def activeConf: BenchmarkConfig = {
    if (_activeConf.isEmpty) {
      assert(false, "No active configuration found!")
    }
    _activeConf.get
  }

  def allTests(): Seq[OapBenchmarkTest] = testSet

  def runAll(repCount: Int): Unit = {
    testSet.foreach{
      run(_, repCount)
    }
  }

  def run(name: String, repCount: Int): Unit = {
    testSet.filter(_.name == name).foreach{
      run(_, repCount)
    }
  }

  def run(test: OapBenchmarkTest, repCount: Int): Unit = {
    logWarning(s"running ${test.name} ($repCount times) ...")
    val result = (1 to repCount).map{ _ =>
      dropCache()
      TestUtil.queryTime(spark.sql(test.sql).foreach{ _ => })
    }.toArray

    val prev: Seq[(String, Array[Int])] = _resultMap.getOrElse(test.name, Nil)
    val curr = prev :+ (activeConf.toString, result)
    _resultMap.put(test.name, curr)
  }

  private var _activeConf: Option[BenchmarkConfig] = None
  def runWith(conf: BenchmarkConfig)(body: => Unit): Unit = {
    _activeConf = Some(conf)
    beforeAll(conf.allSparkOptions())
    if (prepare()){
      body
    } else {
      assert(false, s"$this checkCondition Failed!")
    }
    afterAll()
    _activeConf = None
  }

  /**
   * Prepare running env, include data check, various settings
   * of current(active) benchmark config.
   *
   * @return true if success
   */
  def prepare(): Boolean

  /**
   *  Final table may look like:
   *  +--------+--------+--------+--------+--------+
   *  |        |        |   T1   |   TN   |Avg/Med |
   *  +--------+--------+--------+--------+--------+
   *  |        |config1 |        |        |        |
   *  +  Test1 +--------+--------+--------+--------+
   *  |        |config2 |        |        |        |
   *  +--------+--------+--------+--------+--------+
   *  |        |config1 |        |        |        |
   *  +  Test2 +--------+--------+--------+--------+
   *  |        |config2 |        |        |        |
   *  +--------+--------+--------+--------+--------+
   *
   *  resultMap: (Test1 -> Seq( (config1, (1, 2, 3, ...)),
   *                            (config2, (1, 2, 3, ...))),
   *              Test2 -> Seq( (config1, (1, 2, 3, ...)),
   *                            (config2, (1, 2, 3, ...))),
   *              ...)
   */
  private val _resultMap: mutable.LinkedHashMap[String, Seq[(String, Array[Int])]] =
    new mutable.LinkedHashMap[String, Seq[(String, Array[Int])]]

  def resultMap = _resultMap

  protected def testSet: Seq[OapBenchmarkTest]
  protected def dropCache(): Unit = {
    val nodes = spark.sparkContext.getExecutorMemoryStatus.map(_._1.split(":")(0))
    nodes.foreach { node =>
      val dropCacheResult = Seq("bash", "-c", s"""ssh $node "echo 3 > /proc/sys/vm/drop_caches"""").!
      assert(dropCacheResult == 0)
    }
  }

}

object BenchmarkSuiteSelector extends Logging{

  private val allRegisterSuites = new ArrayBuffer[OapTestSuite]()

  def registerSuite(suite: OapTestSuite) = {
    allRegisterSuites.append(suite)
    logWarning(s"Register $suite")
  }

  def allSuites: Seq[OapTestSuite] = allRegisterSuites

  var wildcardSuite: Option[String] = None

  def build(name: String): Unit = wildcardSuite = Some(name)

  // TODO: regex support
  def selectedSuites(): Seq[OapTestSuite] = wildcardSuite match {
    case Some(name) =>allRegisterSuites.filter(_.toString.contains(name))
    case None => allRegisterSuites
  }
}

object BenchmarkTestSelector {
  private var wildcardTest: Seq[String] = Seq.empty

  def build(name: String): Unit = wildcardTest = name.split(';')

  def selectedTests(): Seq[String] = wildcardTest
}
