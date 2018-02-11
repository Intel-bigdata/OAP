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

import java.io.File
import java.util

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class DataFileSuite extends QueryTest with SharedOapContext {

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
  }

  override def afterEach(): Unit = {}

  test("apply and cache") {
    val data = (0 to 10).map(i => (i, (i + 'a').toChar.toString))
    val schema = new StructType()
    val config = new Configuration()


    withTempPath { dir =>
      val df = spark.createDataFrame(data)
      df.repartition(1).write.format("oap").save(dir.getAbsolutePath)
      val file = listDirectory(dir).get(0)
      val datafile =
        DataFile(file, schema, OapFileFormat.OAP_DATA_FILE_CLASSNAME, config)
      assert(datafile.path == file)
      assert(datafile.schema == schema)
      assert(datafile.configuration == config)
    }

    withTempPath { dir =>
      val df = spark.createDataFrame(data)
      df.repartition(1).write.parquet(dir.getAbsolutePath)
      val file = listDirectory(dir).get(0)
      val datafile =
        DataFile(file, schema, OapFileFormat.PARQUET_DATA_FILE_CLASSNAME, config)
      assert(datafile.path == file)
      assert(datafile.schema == schema)
      assert(datafile.configuration == config)
    }

    assert(DataFile.cacheSize == 2)

    intercept[OapException] {
      DataFile("nofile", schema, "NotExistClass", config)
      assert(DataFile.cacheSize == 2)
    }
  }

  private def listDirectory(path: File): util.List[String] = {
    val result: util.List[String] = new util.ArrayList[String]
    if (path.isDirectory) for (f <- path.listFiles) {
      result.addAll(listDirectory(f))
    }
    else {
      val c: Char = path.getName.charAt(0)
      if (c != '.' && c != '_') result.add(path.getAbsolutePath)
    }
    result
  }

}
