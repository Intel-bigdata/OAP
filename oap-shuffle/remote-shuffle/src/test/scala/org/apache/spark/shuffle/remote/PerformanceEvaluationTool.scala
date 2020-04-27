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

package org.apache.spark.shuffle.remote

import java.io.{File, FileWriter}
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CountDownLatch

import scala.util.Random

import org.apache.commons.cli.{DefaultParser, HelpFormatter, Options}
import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriterPerfEvaluation
import org.apache.spark.util.{ThreadUtils, Utils}

case class PerfOption (
  mappers: Int,
  reducers: Int,
  poolSize: Int,
  rowsPerMapper: Int,
  // Raw data size, before serialization & compression
  shuffleBlockRawSize: Long,
  writersType: String,
  onlyWrite: Boolean,
  storageMasterUri: String,
  shuffleDir: String,
  logLevel: String,
  remoteShuffleNoDelete: Boolean)

object PerformanceEvaluationTool {

  private var mappers: Int = _
  private var reducers: Int = _
  private var poolSize: Int = _
  private var rowsPerMapper: Int = _
  private var lengthPerRow: Int = _

  // Raw data size, before serialization & compression
  private var shuffleBlockRawSize: Long = _
  private var writersType: String = _
  private var onlyWrite: Boolean = _

  private var storageMasterUri: String = _
  private var shuffleDir: String = _
  private var logLevel: String = _
  private var remoteShuffleNoDeletion: Boolean = _

  private var remoteConf: SparkConf = _
  private var sc: SparkContext = _
  private var blockResolverRemote: ShuffleBlockResolver = _

  private var data: Seq[Product2[Int, ByteBuffer]] = _
  // MapStatus(with data size) for each mapper, after serialization & compression
  private var dataSizeInStorage: MapStatus = _

  private val summarizeFilePath = "/tmp/shuffle_perf.log"
  private val propertiesFilePath = "./spark.conf"

  private var successfulWrite: Boolean = true
  private var successfulRead: Boolean = true

  def main(args: Array[String]): Unit = {
    try {
      parse(args) match {
        case Some(PerfOption(m, r, p, n, b, w, o, uri, d, l, no)) =>
          mappers = m
          reducers = r
          poolSize = p
          rowsPerMapper = n
          shuffleBlockRawSize = b
          writersType = w
          onlyWrite = o
          storageMasterUri = uri
          shuffleDir = d
          logLevel = l
          remoteShuffleNoDeletion = no

          // Set two SparkConfs according to the parameters at first, these configurations are
          // going to influence all later tests.
          prepareSparkConf()

          data = generateData()

          prepareEnvForShuffleWrite()
          val timeWrite = benchmarkShuffleWrite()

          printSummaryString(s"$writersType shuffle writer", timeWrite)
          sc.stop()

          if (!onlyWrite && successfulWrite) {
            // Ensure memory pressure doesn't impact shuffle reading tests
            System.gc()

            prepareEnvForShuffleRead()
            val timeRead = benchmarkShuffleRead()

            printSummaryString(s"shuffle reader", timeRead)
          }

        case None =>
      }

    } finally {
      // Clean up
      afterAll()
    }

    def prepareSparkConf(): Unit = {
      remoteConf = createDefaultConf(loadDefaults = true)
        .set("spark.app.id", s"test_${UUID.randomUUID()}")
        .set(RemoteShuffleConf.STORAGE_MASTER_URI, storageMasterUri)
        .set(RemoteShuffleConf.SHUFFLE_FILES_ROOT_DIRECTORY, shuffleDir)
      if (new File(propertiesFilePath).exists()) {
        Utils.loadDefaultSparkProperties(remoteConf, propertiesFilePath)
      }
    }
  }

  private def setSC(): Unit = {
    // Make SparkEnv.get access possible
    sc = new SparkContext("local[1]", "shuffle_writer_remote", remoteConf)
    sc.setLogLevel(logLevel)
  }

  private def prepareEnvForShuffleWrite(): Unit = {
    setSC()
    val perfEvaluationUtil = writersType match {
      case "general" => SortShuffleWriterPerfEvaluation
      case "unsafe" => UnsafeShuffleWriterPerfEvaluation
      case "bypassmergesort" => BypassMergeSortShuffleWriterPerfEvaluation
    }
    perfEvaluationUtil.globalSetup(reducers, storageMasterUri, shuffleDir, remoteConf)

    blockResolverRemote = UnsafeShuffleWriterPerfEvaluation.globalSetup(
      reducers, storageMasterUri, shuffleDir, remoteConf)
  }

  private def prepareEnvForShuffleRead(): Unit = {
    setSC()
    // Reuse previous remote resolver
    ShuffleReaderPerfEvaluation.globalSetup(mappers, blockResolverRemote,
      dataSizeInStorage, storageMasterUri, shuffleDir, remoteConf)
  }

  private def afterAll(): Unit = {
    if (blockResolverRemote != null) {
      // We may want to see the test data
      if (!remoteShuffleNoDeletion) {
        blockResolverRemote.stop()
      }
    }
    if (sc != null) {
      sc.stop()
    }
  }

  private def initOptions(): Options = {
    val options = new Options()
    options.addOption("h", "help", false, "display help message")
    options.addOption("m", "mappers", true, "# of mappers")
    options.addOption("r", "reducers", true, "# of reducers")
    options.addOption("p", "poolSize", true, "# of threads")
    options.addOption("n", "rows", true, "# of rows per mapper")
    options.addOption("b", "shuffleBlockRawSize", true, "Size of a shuffle block")
    options.addOption("w", "writer", true,
      "Type of shuffle writers, general, unsafe or bypassmergesort")
    options.addOption("onlyWrite", "onlyWrite", false, "Only testing shuffle write")
    options.addOption("uri", "storageMasterUri", true, "Master URI, i.e. daos://default:1")
    options.addOption("d", "dir", true, "Shuffle directory")
    options.addOption("l", "log", true, "Log level")
    options.addOption("noDelete", "noDelete", false, "Not deleting shuffle files after testing")

    options
  }

  private def parse(args: Array[String]): Option[PerfOption] = {
    val parser = new DefaultParser
    val options = initOptions()
    val cmd = parser.parse(options, args)
    if (cmd.hasOption("h")) {
      new HelpFormatter().printHelp("PerformanceEvaluationTool", options)
      None
    } else {
      Some(PerfOption(
        cmd.getOptionValue("m", "5").toInt,
        cmd.getOptionValue("r", "5").toInt,
        if (cmd.hasOption("p")) {
          cmd.getOptionValue("p").toInt
        } else {
          cmd.getOptionValue("m", "5").toInt
        },
        cmd.getOptionValue("n", "1000").toInt,
        cmd.getOptionValue("b", "20000").toLong,
        cmd.getOptionValue("w", "unsafe"),
        cmd.hasOption("onlyWrite"),
        cmd.getOptionValue("uri", "file://"),
        cmd.getOptionValue("d", "/tmp"),
        cmd.getOptionValue("l", "WARN"),
        cmd.hasOption("noDelete")
      ))
    }
  }

  private def generateData(): Seq[Product2[Int, ByteBuffer]] = {
    val totalSize: Long = reducers * shuffleBlockRawSize
    lengthPerRow = (totalSize / rowsPerMapper).toInt
    def getLoad = () => {
      val arr = new Array[Byte](lengthPerRow)
      val random = new Random()
      random.nextBytes(arr)
      ByteBuffer.wrap(arr)
    }

    (0 until rowsPerMapper).zip(Seq.fill(rowsPerMapper)(getLoad()))
  }

  private def benchmarkShuffleWrite(): Long = {
    // scalastyle:off

    val pool = ThreadUtils.newDaemonFixedThreadPool(poolSize, "shuffle-write-perf")
    val start = System.currentTimeMillis()
    val done = new CountDownLatch(mappers)
    (0 until mappers).foreach { mapId =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          try {
            // Under mydebug model, saving and printing MapStatus(ranges of each blocks)
            val getMapStatus = if (logLevel.toLowerCase == "mydebug") {
              // Get all status
              true
            } else {
              // All mappers use same data, only getMapStatus for file size calculation once
              if (mapId == 0) true else false
            }
            // Only set dataSizeInStorage once
            val status = runSingleShuffleWrite(mapId, getMapStatus)
            if (status.isDefined) {
              dataSizeInStorage = status.get
              if (logLevel.toLowerCase == "mydebug") {
                val statusString = (0 until reducers).map(status.get.getSizeForBlock).mkString(", ")
                println(s"Mapper: ${mapId}'s mapstatus: ${statusString}")
              }
            }
            println(s"Successfully run iteration i: ${mapId}, Thread: ${Thread.currentThread().getId()}")
          } catch {
            case e: Throwable =>
              println(s"Exception occurs ${e.toString} in mapper: ${mapId}")
              successfulWrite = false
              e.printStackTrace()
          } finally {
            done.countDown()
          }
        }
      })
    }
    // scalastyle:on

    pool.shutdown()
    done.await()
    System.currentTimeMillis() - start
  }

  private def runSingleShuffleWrite(
    mapId: Int, getMapStatus: Boolean = false): Option[MapStatus] = {
    val writer = {
      if (writersType == "general") {
        SortShuffleWriterPerfEvaluation.getRemoteWriter(mapId)
      } else if (writersType == "unsafe") {
        UnsafeShuffleWriterPerfEvaluation.getRemoteWriter(mapId, remoteConf)
      } else {
        BypassMergeSortShuffleWriterPerfEvaluation.getRemoteWriter(mapId, remoteConf)
      }
    }
    writer.write(data.toIterator)
    if (getMapStatus) {
      Some(writer.stop(true).get)
    } else {
      None
    }
  }

  private def benchmarkShuffleRead(): Long = {
    // scalastyle:off

    val pool = ThreadUtils.newDaemonFixedThreadPool(poolSize, "shuffle-write-perf")
    val start = System.currentTimeMillis()
    val done = new CountDownLatch(reducers)
    (0 until reducers).foreach { reduceId =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          try {
            runSingleShuffleRead(reduceId)
            println(s"Successfully run iteration i: ${reduceId}, Thread: ${Thread.currentThread().getId()}")
          } catch {
            case e: Throwable =>
              println(s"Exception occurs ${e.toString} in reducer: ${reduceId}")
              successfulRead = false
              e.printStackTrace()
          } finally {
            done.countDown()
          }
        }
      })
    }
    // scalastyle:on

    pool.shutdown()
    done.await()
    System.currentTimeMillis() - start
  }

  private def runSingleShuffleRead(reduceId: Int): Unit = {
    val reader = ShuffleReaderPerfEvaluation.getRemoteReader(reduceId, reduceId + 1)
    reader.read().length
  }

  private def printSummaryString(name: String, time: Long): Unit = {
    // scalastyle:off
    val success = (name.contains("write") && successfulWrite) || (name.contains("read") && successfulRead)
    if (success) {
      println(s"Successfully run ${name} benchmark in ${(time.toDouble / 1024).formatted("%.1f")} s")
      val rawDataSize: Long = mappers.toLong * reducers * shuffleBlockRawSize
      val compressedDataSizes = (0 until reducers).map(dataSizeInStorage.getSizeForBlock)
      val averageCompressedDataSizePerBlock: Long =
        compressedDataSizes.sum / compressedDataSizes.size
      val compressedDataSize: Long = mappers * compressedDataSizes.sum

      val rawDataThroughput = rawDataSize.toDouble / time / 1024
      val compressedDataThroughput = compressedDataSize.toDouble / time / 1024
      val t = (time.toDouble / 1024).formatted("%.1f")
      val summary =
        s"""|$name:
            |    raw total size:      ${FileUtils.byteCountToDisplaySize(rawDataSize)}
            |    compressed size:     ${FileUtils.byteCountToDisplaySize(compressedDataSize)}
            |    duration:            ${t} seconds
            |
            |    throughput(raw):     ${rawDataThroughput} MB/s
            |    throughput(storage): ${compressedDataThroughput} MB/s
            |
            |    number of mappers:   $mappers
            |    number of reducers:  $reducers
            |    block size(raw):     ${FileUtils.byteCountToDisplaySize(shuffleBlockRawSize)}
            |    block size(storage): ${FileUtils.byteCountToDisplaySize(
          averageCompressedDataSizePerBlock)}
            |
            |    properties:          ${if (new File(propertiesFilePath).exists()) {
          Utils.getPropertiesFromFile(propertiesFilePath)
        } else {
         ""
        }.mkString(", ")}
            |
            |    records per mapper:  $rowsPerMapper
            |    load size per record:$lengthPerRow
            |
            |    shuffle storage      $storageMasterUri
            |    shuffle folder:      $shuffleDir
            |
            |""".stripMargin

      println(summary)
      var writer: FileWriter = null
      try {
        writer = new FileWriter(summarizeFilePath, true )
        writer.write(summary)
        writer.flush()
      } finally {
        if (writer != null) {
          writer.close()
        }
      }

    } else {
      println(s"Failed to run ${name} benchmark")
    }
    // scalastyle:on
  }

}
