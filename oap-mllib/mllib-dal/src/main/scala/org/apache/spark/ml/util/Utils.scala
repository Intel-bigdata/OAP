package org.apache.spark.ml.util

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector

object Utils {

  def profile[R](title: String, block: => R): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    println(s"${title} elapsed: ${end - start} ns")
    result
  }

  // Return index -> (rows, cols) map
  def getPartitionDims(data: RDD[Vector]): Map[Int, (Int, Int)] = {
    var numCols: Int = 0
    // Collect the numRows and numCols
    val collected = data.mapPartitionsWithIndex { (index: Int, it: Iterator[Vector]) =>
      if (it.hasNext) {
        numCols = it.next().size
        Iterator((index, it.size + 1, numCols))
      } else {
        Iterator((index, 0, numCols))
      }
    }.collect

    var ret = Map[Int, (Int, Int)]()

    // set numRows and numCols
    collected.foreach {
      case (index, rows, cols) =>
        ret += (index -> (rows, cols))
    }

    ret
  }

  // Run on Driver
  def sparkExecutorNum(): Int = {
    val conf = new SparkConf()

    val executorNum = conf.getInt("spark.executor.instances", -1)

    assert(executorNum != -1, message = "spark.executor.instances not set")

    executorNum
  }

  def sparkExecutorCores(): Int = {
    val conf = new SparkConf(true)

    // Use 1 if not set
    val executorCores = conf.getInt("spark.executor.cores", 1)

    executorCores
  }

  def sparkFirstExecutorIP(sc: SparkContext): String = {
    val info = sc.statusTracker.getExecutorInfos
    // get first executor, info(0) is driver
    val host = info(1).host()
    val ip = InetAddress.getByName(host).getHostAddress
    ip
  }
}
