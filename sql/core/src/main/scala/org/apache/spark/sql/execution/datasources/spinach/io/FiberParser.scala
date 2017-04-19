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

package org.apache.spark.sql.execution.datasources.spinach.io

import org.apache.parquet.format.Encoding

import org.apache.spark.sql.types._

private[spinach] trait DataFiberParser {
  def parse(bytes: Array[Byte], rowCount: Int): Array[Byte]
}

object DataFiberParser {
  def apply(encoding: Encoding,
            meta: SpinachDataFileHandle,
            dataType: DataType): DataFiberParser = {

    encoding match {
      case Encoding.PLAIN => new PlainDataFiberParser(meta)
      case _ => sys.error(s"Not support encoding type: $encoding")
    }
  }
}
private[spinach] case class PlainDataFiberParser(
  meta: SpinachDataFileHandle) extends DataFiberParser{

  override def parse(bytes: Array[Byte], rowCount: Int): Array[Byte] = bytes
}
