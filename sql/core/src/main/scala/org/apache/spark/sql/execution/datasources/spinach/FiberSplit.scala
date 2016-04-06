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

package org.apache.spark.sql.execution.datasources.spinach

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.types.{DataType, StructType}

import scala.beans.BeanProperty

/** Constructs a split with host information
  *
  * @param length the split length
  * @param file the file name
  * @param hosts the list of hosts containing the block, possibly null
  */
class FiberSplit(@BeanProperty var length: Long,
                 @BeanProperty var file: Path,
                 @BeanProperty var hosts: Array[String]) extends InputSplit with Writable {
  def this() {
    this(0, null, null)
  }

  override def toString: String = {
    s"Length: $length, File:$file, Host:${hosts.mkString}"
  }

  def write(out: DataOutput) {
    out.writeLong(length)
    out.writeUTF(file.toString)
    out.writeInt(hosts.length)
    hosts.foreach(out.writeUTF _)
  }

  def readFields(in: DataInput) {
    length = in.readLong()
    file = new Path(in.readUTF())
    hosts = new Array[String](in.readInt())
    var i = 0
    while (i < hosts.length) {
      hosts(i) = in.readUTF()
      i += 1
    }
  }

  def getLocations: Array[String] = hosts
}
