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

package org.apache.spark.sql.execution.datasources.spinach.index

import java.io.{DataOutputStream, IOException, UnsupportedEncodingException}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.parquet.hadoop.util.ContextUtil

private[index] class SpinachIndexOutputFormat[T] extends FileOutputFormat[Void, T] {

  protected object NoBoundaryRecordWriter {
    private val utf8: String = "UTF-8"
  }

  protected class NoBoundaryRecordWriter[T] extends RecordWriter[Void, T] {
    protected var out: DataOutputStream = null
    private final var keyValueSeparator: Array[Byte] = null

    def this(out: DataOutputStream, keyValueSeparator: String) {
      this()
      this.out = out
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(NoBoundaryRecordWriter.utf8)
      } catch {
        case uee: UnsupportedEncodingException =>
          throw new IllegalArgumentException(
            "can't find " + NoBoundaryRecordWriter.utf8 + " encoding")
      }
    }

    def this(out: DataOutputStream) {
      this(out, "")
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    @throws(classOf[IOException])
    private def writeObject(o: Any): Unit = {
      if (o.isInstanceOf[Text]) {
        val to: Text = o.asInstanceOf[Text]
        out.write(to.getBytes, 0, to.getLength)
      } else if (o.isInstanceOf[Array[Byte]]) {
        out.write(o.asInstanceOf[Array[Byte]])
      } else if (o.isInstanceOf[Int]) {
        out.write(o.asInstanceOf[Int])
      } else {
        out.write(o.toString.getBytes(NoBoundaryRecordWriter.utf8))
      }
    }

    @throws(classOf[IOException])
    def write(value: T): Unit = {
      write(null, value)
    }

    @throws(classOf[IOException])
    def write(key: Void, value: T): Unit = {
      val nullValue: Boolean = value == null
      if (nullValue) {
        return
      }
      if (!nullValue) {
        writeObject(value)
      }
      // no need to write new line
    }

    @throws(classOf[IOException])
    def close(context: TaskAttemptContext) {
      out.close()
    }
  }

  override def getRecordWriter(
      taskAttemptContext: TaskAttemptContext): NoBoundaryRecordWriter[T] = {
    val conf = ContextUtil.getConfiguration(taskAttemptContext)
    // TODO enable index codec
    // val codec = getCodec(taskAttemptContext)
    val extension = ".index"
    val input = conf.get(IndexWriter.INPUT_FILE_NAME)
    val indexName = conf.get(IndexWriter.INDEX_NAME)
    val time = conf.get(IndexWriter.INDEX_TIME)
    // TODO replace '/' with OS specific separator
    val simpleName = input.substring(input.lastIndexOf('/') + 1, input.lastIndexOf('.'))
    val directory = input.substring(0, input.lastIndexOf('/'))
    val outputName =
      directory + "/." + simpleName + "." + time + "." + indexName + extension
    val file = this.getDefaultWorkFile(taskAttemptContext, outputName)
    val fs = file.getFileSystem(conf)
    // TODO maybe compression here
    // overwrite = true for overwriting index files
    new NoBoundaryRecordWriter[T](fs.create(file, true))
  }

  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    // here extension is already unique
    new Path(extension)
  }
}
