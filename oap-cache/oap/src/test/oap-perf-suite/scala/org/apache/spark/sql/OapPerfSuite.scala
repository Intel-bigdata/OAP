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
 *  Entry Point
 */
package org.apache.spark.sql

import org.reflections.Reflections
import scala.collection.mutable
import scala.reflect.runtime.universe

import org.apache.spark.internal.Logging
import org.apache.spark.sql.suites.LocalSparkMasterTestSuite





object OapPerfSuite extends Logging {

  // register all suite
  {
    val reflections = new Reflections("org.apache.spark.sql")
    val allSubSuiteTypes = reflections.getSubTypesOf(classOf[OapTestSuite])
    allSubSuiteTypes.toArray.foreach { suiteType =>
      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule(suiteType.asInstanceOf[Class[OapTestSuite]].getName)
      val obj = runtimeMirror.reflectModule(module)
      val suite = obj.instance.asInstanceOf[OapTestSuite]

      // skip bootstrapping tests.
      if (!suite.isBootStrapping) {
        BenchmarkSuiteSelector.registerSuite(suite)
      }
    }
  }

  private val usage =
      """
        |  /**
        |   * OapBenchmark
        |   * -c Config (oap/parquet, index/non-index, etc.):
        |   * -p Profile (Full, BigData, SmallData, etc.):
        |   * -s SuiteName (All, etc.):
        |   * -t TestName (All, etc.):
        |   * -d Datagen: build data for all test.
        |   * -r Repeat(3):
        |   * -bootstrapping: self test without cluster support.
        |   */
      """.stripMargin

  /**
   * OapBenchmark
   * -c Config (oap/parquet, index/non-index, etc.):
   * -p Profile (Full, BigData, SmallData, etc.):
   * -s SuiteName (All, etc.):
   * -t TestName (All, etc.):
   * -d Datagen: gen data for test.
   * -r Repeat(3):
   * -b bootstrapping: self test without cluster support.
   * TODO: -Dkey.conf=value
   */
  def main(args: Array[String]): Unit = {
    // TODO: use scala getOpts
    if (args.isEmpty) sys.error(usage)

    var i = 0
    var repeat = 3
    while (i < args.length){
      args(i) match {
        case "-suite" | "-s" => {
          assert(args.length > i + 1)
          BenchmarkSuiteSelector.build(args(i + 1))
          i += 2
        }
        case "-config" | "-c" => {
          // TODO: regex check: -c a=b;c=d;e=f
          assert(args.length > i + 1)
          val options: mutable.HashMap[String, String] = mutable.HashMap.empty
          args(i + 1).split(';').map{_.split('=')}.foreach{ kv =>
            options ++= Map(kv(0)->kv(1))
          }
          BenchmarkConfigSelector.build(options.toMap)
          i += 2
        }
        case "-test" | "-t" => {
          assert(args.length > i + 1)
          BenchmarkTestSelector.build(args(i + 1))
          i += 2
        }
        case "-repeat" | "-r" => {
          assert(args.length > i + 1)
          repeat = args(i + 1).toInt
          i += 2
        }
//        case "-datagen" | "-d" => {
//          OapBenchmarkDataBuilder.beforeAll()
//          OapBenchmarkDataBuilder.generateTables()
//          OapBenchmarkDataBuilder.generateDatabases()
//          OapBenchmarkDataBuilder.buildAllIndex()
//          OapBenchmarkDataBuilder.afterAll()
//          // if run with -d only
//          if(i == 0 && args.length == 1){
//            sys.exit()
//          } else {
//            i += 1
//          }
//        }
        case "-bootstrapping" => {
          // self test.
          assert(args.length == i + 1, "bootstrapping works alone.")
          runSuite(LocalSparkMasterTestSuite, 3)
          i += 1
          sys.exit(1)
        }
        case _ => sys.error(usage)
      }
    }

    BenchmarkSuiteSelector.selectedSuites().foreach{suite => runSuite(suite, repeat)}
  }

  /**
   * Run a suite
   * @param suite: OapTestSuite knows how to run itself and
   *               give a report.
   */
  def runSuite(suite: OapTestSuite, repeat: Int = 3): Unit = {
    suite.allConfigurations
      .filter(BenchmarkConfigSelector.isSelected(_))
      .foreach{ conf =>
        suite.runWith(conf){
          logWarning(s"running $suite with conf($conf).")
          if (BenchmarkTestSelector.selectedTests().nonEmpty) {
            BenchmarkTestSelector.selectedTests().foreach{
              suite.run(_, repeat)
            }
          } else {
            suite.runAll(repeat)
          }
        }
      }

    val res = suite.resultMap.toSeq
    if (res.nonEmpty) {
      // Object's Name is XXXXX$, so remove this $
      // TODO: check if $ exists.
      println("#"+ suite.getClass.getCanonicalName.dropRight(1))
      TestUtil.formatResults(res)
    }
  }
}
