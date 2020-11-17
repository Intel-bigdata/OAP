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

import java.nio.DoubleBuffer

import com.intel.daal.data_management.data.{HomogenNumericTable, NumericTable, RowMergedNumericTable, Matrix => DALMatrix}
import com.intel.daal.services.DaalContext
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.{Vector => OldVector}
import org.apache.spark.rdd.{ExecutorInProcessCoalescePartitioner, RDD}
import java.util.logging.{Logger, Level}

object OneDAL {

  private val logger = Logger.getLogger("util.OneDAL")
  private val logLevel = Level.INFO

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

  def rddVectorToNumericTables(vectors: RDD[Vector], executorNum: Int): RDD[Long] = {
    // repartition to executorNum if not enough partitions
    val dataForConversion = if (vectors.getNumPartitions < executorNum) {
      vectors.repartition(executorNum).setName("Repartitioned for conversion").cache()
    } else {
      vectors
    }

    val partitionDims = Utils.getPartitionDims(dataForConversion)

    // filter out empty partitions
    val nonEmptyPartitions = dataForConversion.mapPartitionsWithIndex { (index: Int, it: Iterator[Vector]) =>
      Iterator(Tuple3(partitionDims(index)._1, index, it))
    }.filter { entry => { entry._1 > 0 }}

    val numericTables = nonEmptyPartitions.map { entry =>
      val numRows = entry._1
      val index = entry._2
      val it = entry._3
      val numCols = partitionDims(index)._2

      // Build DALMatrix, this will load libJavaAPI, libtbb, libtbbmalloc
      val context = new DaalContext()
      val matrix = new DALMatrix(context, classOf[java.lang.Double],
        numCols.toLong, numRows.toLong, NumericTable.AllocationFlag.DoAllocate)

      // oneDAL libs should be loaded by now, loading other native libs
      logger.log(logLevel, "IntelMLlib: Loading other native libraries ...")
      LibLoader.loadLibraries()

      var dalRow = 0

      it.foreach { curVector =>
        val rowArray = curVector.toArray
        OneDAL.cSetDoubleBatch(matrix.getCNumericTable, dalRow, rowArray, 1, numCols)
        dalRow += 1
      }

      matrix.getCNumericTable
    }.cache()

    // workaroud to fix the bug of multi executors handling same partition.
    numericTables.foreachPartition(() => _)
    numericTables.count()

    val cachedRdds = vectors.sparkContext.getPersistentRDDs
    cachedRdds.filter(r => r._2.name=="instancesRDD").foreach (r => r._2.unpersist())

    val coalescedRdd = numericTables.coalesce(1,
      partitionCoalescer = Some(new ExecutorInProcessCoalescePartitioner()))

    val coalescedTables = coalescedRdd.mapPartitions { iter =>
      val context = new DaalContext()
      val mergedData = new RowMergedNumericTable(context)

      iter.foreach { address =>
        OneDAL.cAddNumericTable(mergedData.getCNumericTable, address)
      }
      Iterator(mergedData.getCNumericTable)
    }.cache()

    coalescedTables
  }

  @native def setNumericTableValue(numTableAddr: Long, rowIndex: Int, colIndex: Int, value: Double)

  @native def cAddNumericTable(cObject: Long, numericTableAddr: Long)

  @native def cSetDoubleBatch(numTableAddr: Long, curRows: Int, batch: Array[Double], numRows: Int, numCols: Int)
  
  @native def cFreeDataMemory(numTableAddr: Long)

  @native def cCheckPlatformCompatibility() : Boolean
}
