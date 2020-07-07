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

package com.intel.hibench.sparkbench.ml

import breeze.linalg.DenseVector
import org.apache.hadoop.io.LongWritable
import org.apache.mahout.math.VectorWritable
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import scopt.OptionParser
import org.apache.spark.sql.SparkSession

/**
  *
  * An example k-means app. Run with
  * {{{
  * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
  * }}}
  * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
  */
object DenseKMeansDS {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import com.intel.hibench.sparkbench.ml.DenseKMeansDS.InitializationMode._

  case class Params(input: String = null,
                    k: Int = -1,
                    numIterations: Int = 10,
                    initializationMode: InitializationMode = Parallel)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("DenseKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default; ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
          s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      arg[String]("<input>")
        .text("input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val spark = SparkSession
      .builder
      .appName(s"DenseKMeansDS with $params")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    val data = sc.sequenceFile[LongWritable, VectorWritable](params.input)

    // Should use Tuple1 to warp around for calling toDF
    val dataset = data.map { case (k, v) =>
      var vector: Array[Double] = new Array[Double](v.get().size)
      for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
      Tuple1(Vectors.dense(vector))
    }.toDF("features")

    val numExamples = dataset.count()

    println(s"numExamples = $numExamples.")

    val initMode = params.initializationMode match {
      case Random => "random"
      case Parallel => "k-means||"
    }

    val model = new KMeans()
      .setInitMode(initMode)
      .setK(params.k)
      .setMaxIter(params.numIterations)
      .setSeed(1L)
      .fit(dataset)

    val cost = model.summary.trainingCost

    println(s"Total cost = $cost.")

    spark.stop()
  }
}
