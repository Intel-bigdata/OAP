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

package org.apache.spark.ml.util

import com.intel.daal.data_management.data.{HomogenNumericTable, NumericTable, Matrix => DALMatrix}
import com.intel.daal.services.DaalContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Vector => OldVector}

object OneDAL {

  // Convert DAL numeric table to array of vectors
  def numericTableToVectors(table: NumericTable): Array[Vector] = {
    val numRows = table.getNumberOfRows.toInt
    val numCols = table.getNumberOfColumns.toInt

    val resArray = new Array[Vector](numRows.toInt)

    for (row <- 0 until numRows) {
      val internArray = new Array[Double](numCols)
      for (col <- 0 until numCols) {
        internArray(col) = table.getDoubleValue(col, row)
      }
      resArray(row) = Vectors.dense(internArray)
    }

    resArray
  }

  def makeNumericTable (cData: Long) : NumericTable = {

    val context = new DaalContext()
    val table = new HomogenNumericTable(context, cData)

    table
  }

  def makeNumericTable (arrayVectors: Array[OldVector]): NumericTable = {

    val numCols = arrayVectors.head.size
    val numRows: Int = arrayVectors.size

    val context = new DaalContext()
    val matrix = new DALMatrix(context, classOf[java.lang.Double],
      numCols.toLong, numRows.toLong, NumericTable.AllocationFlag.DoAllocate)

    arrayVectors.zipWithIndex.foreach {
      case (v, rowIndex) =>
        for (colIndex <- 0 until numCols)
        // matrix.set(rowIndex, colIndex, row.getString(colIndex).toDouble)
          setNumericTableValue(matrix.getCNumericTable, rowIndex, colIndex, v(colIndex))
    }

    matrix
  }

  @native def setNumericTableValue(numTableAddr: Long, rowIndex: Int, colIndex: Int, value: Double)
  
  @native def cSetDoubleIterator(numTableAddr: Long, iter: java.util.Iterator[DataBatch])

  @native def cAddNumericTable(cObject: Long, numericTableAddr: Long)
}
