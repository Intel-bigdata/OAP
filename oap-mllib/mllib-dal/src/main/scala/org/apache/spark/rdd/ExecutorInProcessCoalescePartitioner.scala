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

package org.apache.spark.rdd

import org.apache.commons.logging.LogFactory

import org.apache.spark.Partition
import org.apache.spark.SparkException
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.scheduler.TaskLocation

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ExecutorInProcessCoalescePartitioner
  extends PartitionCoalescer with Serializable {

  def coalesce(maxPartitions: Int, prev: RDD[_]): Array[PartitionGroup] = {
    val map = new mutable.HashMap[String, mutable.HashSet[Partition]]()

    val groupArr = ArrayBuffer[PartitionGroup]()
    prev.partitions.foreach(p => {
      val loc = prev.context.getPreferredLocs(prev, p.index)
      loc.foreach{
      case location : ExecutorCacheTaskLocation =>
     
        val execLoc = "executor_" + location.host + "_" + location.executorId
        val partValue = map.getOrElse(execLoc, new mutable.HashSet[Partition]())
        partValue.add(p)
        map.put(execLoc, partValue)
      case loc : TaskLocation =>
	    throw new SparkException("Invalid location !!!")

      }
    })
    map.foreach(x => {
      val pg = new PartitionGroup(Some(x._1))
      val list = x._2.toList.sortWith(_.index < _.index);
      list.foreach(part => pg.partitions += part)
      groupArr += pg
    })
    if (groupArr.length == 0) throw new SparkException("No partitions or" +
      " no locations for partitions found.")

    val sortedGroupArr = groupArr.sortWith(_.partitions(0).index < _.partitions(0).index)

    return sortedGroupArr.toArray
  }
}

