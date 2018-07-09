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

package org.apache.spark.sql.execution.vectorized

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.IntegerType

class OnHeapCoumnVectorFiledAccessorSuite extends SparkFunSuite with Logging {
  test("test getFieldValue with valid field") {
    val count = 100
    val vector = new OnHeapColumnVector(count, IntegerType)
    (0 until count).foreach(i => vector.putInt(i, i * 2))
    val intData = OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "intData")
    assert(intData.isInstanceOf[Array[Int]])
    val intArray = intData.asInstanceOf[Array[Int]]
    (0 until count).foreach(i => assert(intArray(i) == i * 2))
  }

  test("test getFieldValue with invalid field") {
    val count = 100
    val vector = new OnHeapColumnVector(count, IntegerType)
    (0 until count).foreach(i => vector.putInt(i, i * 2))
    val exception = intercept[IllegalArgumentException] {
      OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "int")
    }
    assert(exception.getMessage.contains("Could not find field"))
    assert(OnHeapCoumnVectorFiledAccessor.getFieldValue(vector, "longData") == null)
  }

  test("test setFieldValue with valid field") {
    val count = 100
    val vector = new OnHeapColumnVector(count, IntegerType)
    val intData = new Array[Int](count)
    (0 until count).foreach(i => intData(i) = i * 2)
    OnHeapCoumnVectorFiledAccessor.setFieldValue(vector, "intData", intData)
    (0 until count).foreach(i => assert(vector.getInt(i) == i * 2))
  }

  test("test setFieldValue with invalid field") {
    val count = 100
    val vector = new OnHeapColumnVector(count, IntegerType)
    val intData = new Array[Int](count)
    (0 until count).foreach(i => intData(i) = i * 2)
    val exception = intercept[IllegalArgumentException] {
      OnHeapCoumnVectorFiledAccessor.setFieldValue(vector, "int", intData)
    }
    assert(exception.getMessage.contains("Could not find field"))
    val runtimeException = intercept[RuntimeException] {
      OnHeapCoumnVectorFiledAccessor.setFieldValue(vector, "nulls", intData)
    }
    assert(runtimeException.getMessage.contains("Can not set"))
  }
}
