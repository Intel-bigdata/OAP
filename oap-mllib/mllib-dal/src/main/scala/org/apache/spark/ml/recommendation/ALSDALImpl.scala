package org.apache.spark.ml.recommendation

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import com.intel.daal.data_management.data.{CSRNumericTable, HomogenNumericTable, Matrix => DALMatrix}
import com.intel.daal.services.DaalContext
import org.apache.spark.internal.Logging
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.util.{Service, Utils}

class ALSDALImpl[@specialized(Int, Long) ID: ClassTag](
  data: RDD[Rating[ID]],
  rank: Int,
  maxIter: Int,
  regParam: Double,
  alpha: Double,
  seed: Long,
) extends Serializable with Logging {

  // Return Map partitionId -> (ratingsNum, csrRowNum)
  private def getRatingsPartitionInfo(data: RDD[Rating[ID]]): Map[Int, (Int, Int)] = {
    val collectd = data.mapPartitionsWithIndex { case (index: Int, it: Iterator[Rating[ID]]) =>
      var ratingsNum = 0
      var s = Set[ID]()
      it.foreach { v =>
        s += v.user
        ratingsNum += 1
      }
      Iterator((index, (ratingsNum, s.count(_ => true))))
    }.collect

    var ret = Map[Int, (Int, Int)]()
    collectd.foreach { v =>
      ret += (v._1 -> v._2)
    }

    ret
  }

  private def ratingsToCSRNumericTables(ratings: RDD[Rating[ID]],
    nRatings: Long, nVectors: Long, nFeatures: Long): RDD[CSRNumericTable] = {

    val ratingsPartitionInfo = getRatingsPartitionInfo(ratings)

    val rowSortedRatings = ratings.sortBy(_.user.toString.toLong)

//    println("ratingsToCSRNumericTables", nRatings, nVectors, nFeatures)

    // need to modify each index substract previous index
    rowSortedRatings.mapPartitionsWithIndex { case (partitionId, partition) =>
      val ratingsNum = ratingsPartitionInfo(partitionId)._1
      val csrRowNum = ratingsPartitionInfo(partitionId)._2
      val values = Array.fill(ratingsNum) { 0.0f }
      val columnIndices = Array.fill(ratingsNum) { 0L }
      val rowOffsets = Array.fill(csrRowNum) { 0L }

      var index = 0
      var curRow = 0
      // Each partition converted to one CSRNumericTable
      partition.foreach { p =>
        val row = p.user.toString.toLong
        val column = p.item.toString.toLong
        val rating = p.rating

        values(index) = rating
        columnIndices(index) = column

        if (row > rowOffsets(curRow)) {
          curRow = curRow + 1
          rowOffsets(curRow) = index
        }

        index = index + 1
      }
      curRow = curRow + 1
      rowOffsets(curRow) = index

      println("rowOffsets", rowOffsets.mkString(","))
      println("columnIndices", columnIndices.mkString(","))
      println("values", values.mkString(","))

      val contextLocal = new DaalContext()
      val table = new CSRNumericTable(contextLocal, values, columnIndices, rowOffsets, nFeatures, csrRowNum)

      Service.printNumericTable("Input", table)

      Iterator(table)
    }
  }

  def run(): (RDD[(ID, Array[Float])], RDD[(ID, Array[Float])]) = {
    val executorNum = Utils.sparkExecutorNum()
    val executorCores = Utils.sparkExecutorCores()

    val largestItems = data.sortBy(_.item.toString.toLong, ascending = false).take(1)
    val nFeatures = largestItems(0).item.toString.toLong + 1

    val largestUsers = data.sortBy(_.user.toString.toLong, ascending = false).take(1)
    val nVectors = largestUsers(0).user.toString.toLong + 1

    val nRatings = data.count()

    logInfo(s"ALSDAL fit $nRatings ratings using $executorNum Executors for $nVectors vectors and $nFeatures features")

    val executorIPAddress = Utils.sparkFirstExecutorIP(data.sparkContext)
    val dataForConversion = if (data.getNumPartitions < executorNum) {
      data.repartition(executorNum).setName("Repartitioned for conversion").cache()
    } else {
      data
    }

    val numericTables = ratingsToCSRNumericTables(dataForConversion, nRatings, nVectors, nFeatures)
    numericTables.count()

    null
  }

  // Single entry to call Implict ALS DAL backend
  @native private def cDALImplictALS(data: Long, 
                                     nUsers: Long,
                                     rank: Int,
                                     maxIter: Int,
                                     regParam: Double,
                                     alpha: Double,
                                     executor_num: Int,
                                     executor_cores: Int,
                                     result: ALSResult): Long

}