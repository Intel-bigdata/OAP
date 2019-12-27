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


package org.apache.spark.sql.execution.datasources.oap.filecache

import com.google.common.util.concurrent.ExecutionError
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopStreams

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.oap.io.TestDataFile
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.sql.types.StructType

class BinaryFiberIdSuite extends SharedOapContext with Logging {

  test("test binary fiber id write and read") {
    withFileSystem(fs => withTempPath(path => {
      // prepare data
      val data = "test binary fiber id write and read".getBytes
      val file = new Path(path.getAbsolutePath)
      val output = fs.create(file)
      output.write(data)
      output.close()

      val input = HadoopStreams.wrap(fs.open(file))
      val dataFile = TestDataFile(file.getName, new StructType(), configuration)
      val binaryDataFiberId = BinaryDataFiberId(dataFile, columnIndex = 0, rowGroupId = 0)
      val cacheManager = OapRuntime.getOrCreate.fiberCacheManager

      // Test input is null
      binaryDataFiberId.withLoadCacheParameters(input = null, 0, 10)
      val e1 = intercept[ExecutionError](cacheManager.get(binaryDataFiberId))
      assert(e1.getMessage.contains("Illegal condition when load binary Fiber to cache."))
      binaryDataFiberId.cleanLoadCacheParameters()


      // Test offset < 0
      binaryDataFiberId.withLoadCacheParameters(input, offset = -1, 10)
      val e2 = intercept[ExecutionError](cacheManager.get(binaryDataFiberId))
      assert(e2.getMessage.contains("Illegal condition when load binary Fiber to cache."))
      binaryDataFiberId.cleanLoadCacheParameters()

      // Test length <= 0
      binaryDataFiberId.withLoadCacheParameters(input, offset = 0, length = 0)
      val e3 = intercept[ExecutionError](cacheManager.get(binaryDataFiberId))
      assert(e3.getMessage.contains("Illegal condition when load binary Fiber to cache."))
      binaryDataFiberId.cleanLoadCacheParameters()

      // Test normal case
      binaryDataFiberId.withLoadCacheParameters(input, 0, 10)
      val fiberCache = cacheManager.get(binaryDataFiberId)
      input.close()
      assert(fiberCache != null)
      assert(fiberCache.size() == 10)
      fiberCache.release()
      binaryDataFiberId.cleanLoadCacheParameters()
      assert(fiberCache.refCount == 0)
      assert(cacheManager.getIfPresent(binaryDataFiberId) != null)
      cacheManager.clearAllFibers()
      assert(cacheManager.getIfPresent(binaryDataFiberId) == null)
    }))
  }
}
