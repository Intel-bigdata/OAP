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

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainBinaryDictionary
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache.StringFiberBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class DictionaryBasedEncoderCheck extends Properties("DictionaryBasedEncoder") {
  private val rowCountInEachGroup = Gen.choose(1, 1024)
  private val rowCountInLastGroup = Gen.choose(1, 1024)
  private val groupCount = Gen.choose(1, 100)

  property("Encoding/Decoding String Type") = forAll { (values: Array[String]) =>

    forAll(rowCountInEachGroup, rowCountInLastGroup, groupCount) {
      (rowCount, lastCount, groupCount) =>
        if (values.nonEmpty) {
          // This is the 'PLAIN' FiberBuilder to validate the 'Encoding/Decoding'
          // Normally, the test case should be:
          // values => encoded bytes => decoded bytes => decoded values (Using ColumnValues class)
          // Validate if 'values' and 'decoded values' are identical.
          // But ColumnValues only support read value form DataFile. So, we have to use another way
          // to validate.
          val referenceFiberBuilder = StringFiberBuilder(rowCount, 0)
          val fiberBuilder = PlainBinaryDictionaryFiberBuilder(rowCount, 0, StringType)
          !(0 until groupCount).exists { group =>
            // If lastCount > rowCount, assume lastCount = rowCount
            val count =
              if (group < groupCount - 1) {
                rowCount
              } else if (lastCount > rowCount) {
                rowCount
              } else {
                lastCount
              }
            (0 until count).foreach { row =>
              fiberBuilder.append(InternalRow(UTF8String.fromString(values(row % values.length))))
              referenceFiberBuilder
                .append(InternalRow(UTF8String.fromString(values(row % values.length))))
            }
            val bytes = fiberBuilder.build().fiberData
            val dictionary = new PlainBinaryDictionary(
              new DictionaryPage(
                BytesInput.from(fiberBuilder.buildDictionary),
                fiberBuilder.getDictionarySize,
                org.apache.parquet.column.Encoding.PLAIN))
            val fiberParser = PlainDictionaryFiberParser(
              new OapDataFileMeta(rowCountInEachGroup = rowCount), dictionary, StringType)
            val parsedBytes = fiberParser.parse(bytes, count)
            val referenceBytes = referenceFiberBuilder.build().fiberData
            referenceFiberBuilder.clear()
            referenceFiberBuilder.resetDictionary()
            fiberBuilder.clear()
            fiberBuilder.resetDictionary()
            assert(parsedBytes.length == referenceBytes.length)
            parsedBytes.zip(referenceBytes).exists(byte => byte._1 != byte._2)
          }
        } else {
          true
        }
    }
  }
}

class DictionaryBasedEncoderSuite extends SparkFunSuite with Checkers {

  test("Check Encoding/Decoding") {
    check(new DictionaryBasedEncoderCheck)
  }
}
