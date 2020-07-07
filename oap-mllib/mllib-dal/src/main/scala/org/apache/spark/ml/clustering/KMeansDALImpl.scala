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

package org.apache.spark.ml.clustering

import com.intel.daal.algorithms.KMeansResult
import com.intel.daal.data_management.data.{NumericTable, Matrix => DALMatrix}
import com.intel.daal.services.DaalContext
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{DistanceMeasure, KMeansModel => MLlibKMeansModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.OneDAL._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.rdd.RDD

class KMeansDALImpl (
  var nClusters : Int = 4,
  var maxIterations : Int = 10,
  var tolerance : Double = 1e-6,
  val distanceMeasure: String = DistanceMeasure.EUCLIDEAN,
  val centers: Array[OldVector] = null,
  val executorNum: Int,
  val executorCores: Int
) extends Serializable {

  def runWithRDDVector(data: RDD[Vector], instr: Option[Instrumentation]) : MLlibKMeansModel = {

    instr.foreach(_.logInfo(s"Processing partitions with $executorNum executors"))

    val partitionDims = Utils.getPartitionDims(data)
    val executorIPAddress = Utils.sparkFirstExecutorIP(data.sparkContext)

    val results = data.mapPartitionsWithIndex { (index: Int, it: Iterator[Vector]) =>

    val numRows = partitionDims(index)._1
    val numCols = partitionDims(index)._2

    println(s"KMeansDALImpl: Partition index: $index, numCols: $numCols, numRows: $numRows")

    // Build DALMatrix, this will load libJavaAPI, libtbb, libtbbmalloc
    val context = new DaalContext()
    val localData = new DALMatrix(context, classOf[java.lang.Double],
      numCols.toLong, numRows.toLong, NumericTable.AllocationFlag.DoAllocate)

    println("KMeansDALImpl: Loading libMLlibDAL.so" )
    // oneDAL libs should be loaded by now, extract libMLlibDAL.so to temp file and load
    LibLoader.loadLibrary()

    println(s"KMeansDALImpl: Start data conversion")

    val start = System.nanoTime
    it.zipWithIndex.foreach {
      case (v, rowIndex) =>
        for (colIndex <- 0 until numCols)
          // TODO: Add matrix.set API in DAL to replace this
          // matrix.set(rowIndex, colIndex, row.getString(colIndex).toDouble)
          OneDAL.setNumericTableValue(localData.getCNumericTable, rowIndex, colIndex, v(colIndex))
    }

    val duration = (System.nanoTime - start) / 1E9

    println(s"KMeansDALImpl: Data conversion takes $duration seconds")

    OneCCL.init(executorNum, executorIPAddress, OneCCL.KVS_PORT)

    val initCentroids = OneDAL.makeNumericTable(centers)
    var result = new KMeansResult()
    val cCentroids = cKMeansDALComputeWithInitCenters(
      localData.getCNumericTable,
      initCentroids.getCNumericTable,
      nClusters,
      maxIterations,
      executorNum,
      executorCores,
      result
    )

    val ret = if (OneCCL.isRoot()) {
        assert(cCentroids != 0)

        val centerVectors = OneDAL.numericTableToVectors(OneDAL.makeNumericTable(cCentroids))
        Iterator((centerVectors, result.totalCost))
      } else {
        Iterator.empty
      }

      OneCCL.cleanup()

      ret

    }.collect()

    // Make sure there is only one result from rank 0
    assert(results.length == 1)

    val centerVectors = results(0)._1
    val totalCost = results(0)._2

    instr.foreach(_.logInfo(s"OneDAL output centroids:\n${centerVectors.mkString("\n")}"))

    // TODO: tolerance support in DAL
    val iteration = maxIterations

    val parentModel = new MLlibKMeansModel(
      centerVectors.map(OldVectors.fromML(_)),
      distanceMeasure, totalCost, iteration)

    parentModel
  }

  // Single entry to call KMeans DAL backend with initial centers, output centers
  @native private def cKMeansDALComputeWithInitCenters(data: Long, centers: Long,
                                                       cluster_num: Int, iteration_num: Int,
                                                       executor_num: Int,
                                                       executor_cores: Int,
                                                       result: KMeansResult): Long

}
