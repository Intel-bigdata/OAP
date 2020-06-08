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

package org.apache.spark.sql.execution.datasources.oap.utils

import java.io.OutputStream

case object OutputStreamUtil {
  val INT_SIZE = 4

  def writeBoolean(out: OutputStream, v: Boolean): Unit = out.write(if (v) 1 else 0)

  def writeByte(out: OutputStream, v: Int): Unit = out.write(v)

  def writeBytes(out: OutputStream, b: Array[Byte]): Unit = out.write(b)

  def writeShort(out: OutputStream, v: Int): Unit = {
    out.write(v >>> 0 & 0XFF)
    out.write(v >>> 8 & 0xFF)
  }

  def writeInt(out: OutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
  }

  def writeLong(out: OutputStream, v: Long): Unit = {
    out.write((v >>>  0).toInt & 0xFF)
    out.write((v >>>  8).toInt & 0xFF)
    out.write((v >>> 16).toInt & 0xFF)
    out.write((v >>> 24).toInt & 0xFF)
    out.write((v >>> 32).toInt & 0xFF)
    out.write((v >>> 40).toInt & 0xFF)
    out.write((v >>> 48).toInt & 0xFF)
    out.write((v >>> 56).toInt & 0xFF)
  }

  def writeFloat(out: OutputStream, v: Float): Unit =
    writeInt(out, java.lang.Float.floatToIntBits(v))

  def writeDouble(out: OutputStream, v: Double): Unit =
    writeLong(out, java.lang.Double.doubleToLongBits(v))
}
