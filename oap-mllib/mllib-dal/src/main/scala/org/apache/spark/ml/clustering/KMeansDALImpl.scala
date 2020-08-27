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
import com.intel.daal.data_management.data.{NumericTable, HomogenNumericTable, RowMergedNumericTable, Matrix => DALMatrix}
import com.intel.daal.services.DaalContext
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{DistanceMeasure, KMeansModel => MLlibKMeansModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.OneDAL._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.rdd.{ExecutorInProcessCoalescePartitioner, RDD}
import scala.collection.mutable.ArrayBuffer

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

    val executorIPAddress = Utils.sparkFirstExecutorIP(data.sparkContext)

    // repartition to executorNum if not enough partitions
    val dataForConversion = if (data.getNumPartitions < executorNum) {
	  data.repartition(executorNum).setName("Repartitioned for conversion").cache()
    } else {
      data
    }

    val partitionDims = Utils.getPartitionDims(dataForConversion)

    // filter the empty partitions
    val partRows = dataForConversion.mapPartitionsWithIndex { (index: Int, it: Iterator[Vector]) =>
      Iterator(Tuple3(partitionDims(index)._1, index, it))
    }
    val nonEmptyPart = partRows.filter{entry => { entry._1 > 0 }}
    
    // convert RDD[Vector] to RDD[HomogenNumericTable]
    val numericTables = nonEmptyPart.map { entry =>
      val numRows = entry._1
      val index = entry._2
      val it = entry._3
      val numCols = partitionDims(index)._2
	  
      println(s"KMeansDALImpl: Partition index: $index, numCols: $numCols, numRows: $numRows")

      // Build DALMatrix, this will load libJavaAPI, libtbb, libtbbmalloc
      val context = new DaalContext()
      val matrix = new DALMatrix(context, classOf[java.lang.Double],
        numCols.toLong, numRows.toLong, NumericTable.AllocationFlag.DoAllocate)

      println("KMeansDALImpl: Loading libMLlibDAL.so" )
      // oneDAL libs should be loaded by now, extract libMLlibDAL.so to temp file and load
      LibLoader.loadLibrary()

      import scala.collection.JavaConverters._

      var dalRow = 0
         
      it.foreach { curVector =>
        val rowarr = curVector.toArray
        OneDAL.cSetDoubleBatch(matrix.getCNumericTable, dalRow, rowarr, 1, numCols)
        dalRow += 1
      }

      Iterator(matrix.getCNumericTable)

    }.cache()
    
	// workaroud to fix the bug of multi executors handling same partition.
    numericTables.foreachPartition(() => _)
    numericTables.count()

    val cachedRdds = data.sparkContext.getPersistentRDDs
    cachedRdds.filter(r => r._2.name=="instancesRDD").foreach (r => r._2.unpersist())
	
    val coalescedRdd = numericTables.coalesce(1,
      partitionCoalescer = Some(new ExecutorInProcessCoalescePartitioner()))

    val coalescedTables = coalescedRdd.mapPartitions { iter =>
      val context = new DaalContext()
      val mergedData = new RowMergedNumericTable(context)
	  
      iter.foreach{ curIter =>
        val address = curIter.next()
        OneDAL.cAddNumericTable(mergedData.getCNumericTable, address )
      } 
      Iterator(mergedData.getCNumericTable)
    
    }.cache()

    val results = coalescedTables.mapPartitions { table =>
      val tableArr = table.next()
      OneCCL.init(executorNum, executorIPAddress, OneCCL.KVS_PORT)

      val initCentroids = OneDAL.makeNumericTable(centers)
      var result = new KMeansResult()
      val cCentroids = cKMeansDALComputeWithInitCenters(
        tableArr,
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

    // Release the native memory allocated by NumericTable.
    numericTables.foreach( tables =>
      tables.foreach { address =>
        OneDAL.cFreeDataMemory(address)
      }
    )

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
