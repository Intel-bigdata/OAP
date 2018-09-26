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

package org.apache.spark.sql.parquet.hadoop

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.IndexedVectorizedOapRecordReader
import org.mockito.Mockito

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.test.oap.SharedOapContext

class IndexedVectorizedOapReaderSuite extends QueryTest with ParquetTest with SharedOapContext {

  /**
   * The Test data has one RowGroup and divide into ColumnarBatch may as following:
   * ----------------------
   * | no index hit batch | --> call skipBatchInternal to skip
   * ----------------------
   * | hit 5000 batch     | --> call nextBatchInternal() && filterRowsWithIndex(ids)
   * ----------------------
   * | no index hit batch | --> call skipBatchInternal to skip
   * ----------------------
   * | hit 16000 batch    | --> call nextBatchInternal() && filterRowsWithIndex(ids)
   * ----------------------
   * | no index hit batch | --> discard directly because of idsMap.isEmpty()
   * ----------------------
   * Now this case a little trick, we use spy model to mock IndexedVectorizedOapReader object,
   * it will call real method and we only verify call times of Key methods.
   */
  test("IndexedVectorizedOapRecordReader - nextBatch") {
    val batchSize = 4096
    val data = (0 to batchSize * 5).map(i => (i, (i + 'a').toChar.toString))
    val rowIds = Array(5000, 16000)
    val expected = data.filter( item => rowIds.contains(item._1))
    withTempPath { dir =>
      val df = spark.createDataFrame(data)
      val schema = df.schema.json
      df.repartition(1).write.parquet(dir.getCanonicalPath)
      val file = SpecificParquetRecordReaderBase.listDirectory(dir).get(0)
      val path = new Path(file.asInstanceOf[String])

      configuration.set(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA, schema)

      // TODO refactor IndexedVectorizedOapReader to use Mockito.mock
      val reader = Mockito.spy(new TestIndexedVectorizedOapReader(path, configuration, rowIds))

      try {
        reader.initialize()
        val result = mutable.ArrayBuffer.empty[(Int, String)]
        while (reader.nextKeyValue()) {
          val row = reader.getCurrentValue.asInstanceOf[InternalRow]
          val v = (row.getInt(0), row.getString(1))
          result += v
        }
        assert(expected == result)
        // nextKeyValue call 3 times, result are [true , true , false]
        Mockito.verify(reader, Mockito.times(3)).nextKeyValue()
        // getCurrentValue call 2 times, because of we need only 2 batch of 5
        Mockito.verify(reader, Mockito.times(2)).getCurrentValue
        // nextBatch call 3 times, result are [true , true , false]
        Mockito.verify(reader, Mockito.times(3)).nextBatch()
        // nextBatchInternal call 2 times, because of we need only 2 batch of 5
        Mockito.verify(reader, Mockito.times(2)).nextBatchInternal()
        // skipBatchInternal call 2 times, last batch discard directly because of idsMap.isEmpty()
        Mockito.verify(reader, Mockito.times(2)).skipBatchInternal()
      } finally {
        reader.close()
        configuration.unset(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
      }
    }
  }
}

/**
 * Change nextBatchInternal & skipBatchInternal Modifier,
 * then we can verify call times of Key methods.
 */
class TestIndexedVectorizedOapReader(
    file: Path,
    configuration: Configuration,
    globalRowIds: Array[Int])
  extends IndexedVectorizedOapRecordReader(file, configuration, globalRowIds) {

  override def nextBatchInternal(): Unit = super.nextBatchInternal()

  override def skipBatchInternal(): Unit = super.skipBatchInternal()
}
