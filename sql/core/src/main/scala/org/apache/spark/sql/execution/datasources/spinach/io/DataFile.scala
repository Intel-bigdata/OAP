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

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.parquet.column.Dictionary

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.SpinachException
import org.apache.spark.sql.execution.datasources.spinach.Key
import org.apache.spark.sql.execution.datasources.spinach.filecache.DataFiberCache
import org.apache.spark.sql.execution.datasources.spinach.index.RangeInterval
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

abstract class DataFile {
  def path: String
  def schema: StructType

  def createDataFileHandle(conf: Configuration): DataFileHandle
  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): DataFiberCache
  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow]
  def iterator(conf: Configuration, requiredIds: Array[Int], rowIds: Array[Long])
  : Iterator[InternalRow]
  def getDictionary(fiberId: Int, conf: Configuration): Dictionary
}

private[spinach] object DataFile {
  def apply(path: String, schema: StructType, dataFileClassName: String): DataFile = {
    Try(Utils.classForName(dataFileClassName).getDeclaredConstructor(
      classOf[String], classOf[StructType])).toOption match {
      case Some(ctor) =>
        Try (ctor.newInstance(path, schema).asInstanceOf[DataFile]) match {
          case Success(e) => e
          case Failure(e) =>
            throw new SpinachException(s"Cannot instantiate class $dataFileClassName", e)
        }
      case None => throw new SpinachException(
        s"Cannot find constructor of signature like:" +
          s" (String, StructType) for class $dataFileClassName")
    }
  }
  def encodeKey(dictionaries: Array[Dictionary], schema: StructType, key: Key): Key = {
    val values = schema.zipWithIndex.map {
      case (field, ordinal) =>
        val dict = dictionaries(ordinal)
        if (dict != null) {
          (0 until dictionaries(ordinal).getMaxId).find { i =>
            field.dataType match {
              case StringType =>
                UTF8String.fromBytes(dict.decodeToBinary(i).getBytes)
                  .equals(key.getUTF8String(ordinal))
              case IntegerType =>
                dict.decodeToInt(i) == key.getInt(ordinal)
              case dataType => sys.error(s"not support data type: $dataType")
            }
          } match {
            case Some(value) => value
            case None => -1
          }
        } else {
          key.get(ordinal, field.dataType)
        }
    }
    InternalRow.fromSeq(values)
  }

  def encodeSchema(dictionaries: Array[Dictionary], schema: StructType): StructType = {
    val fields = schema.zipWithIndex.map{
      case (field, ordinal) =>
        if (dictionaries(ordinal) == null) field
        else StructField(field.name, IntegerType, field.nullable)
    }
    StructType(fields)
  }

  // TODO: [linhong] Need Re-write this Ugly Code!
  def encodeInterval(dictionaries: Array[Dictionary],
                     schema: StructType,
                     intervalArray: ArrayBuffer[RangeInterval]): ArrayBuffer[RangeInterval] = {

    if (intervalArray.isEmpty) return ArrayBuffer.empty

    val num = intervalArray.head.start.numFields
    val keys = intervalArray.head.start
    val prefixValues: Seq[Any] = schema.dropRight(1).zipWithIndex.map {
      case (field, ordinal) =>
        val dict = dictionaries(ordinal)
        if (dict != null) {
          (0 until dictionaries(ordinal).getMaxId).find { i =>
            field.dataType match {
              case StringType =>
                UTF8String.fromBytes(dict.decodeToBinary(i).getBytes)
                  .equals(keys.getUTF8String(ordinal))
              case IntegerType =>
                dict.decodeToInt(i) == keys.getInt(ordinal)
              case dataType => sys.error(s"not support data type: $dataType")
            }
          } match {
            case Some(value) => value
            case None => -1
          }
        } else {
          keys.get(ordinal, field.dataType)
        }
    }

    val dict = dictionaries.last
    if (dict != null) {
      intervalArray.flatMap{interval =>
        (0 until dict.getMaxId).filter{ i =>
          schema.last.dataType match {
            case StringType =>
              val v = UTF8String.fromBytes(dict.decodeToBinary(i).getBytes)
              val start = interval.start.getUTF8String(num - 1)
              val end = interval.end.getUTF8String(num - 1)
              (v.compare(start) > 0 && v.compare(end) < 0) ||
                (interval.startInclude && v.compare(start) == 0) ||
                (interval.endInclude && v.compare(end) == 0)
            case IntegerType =>
              val v = dict.decodeToInt(i)
              val start = interval.start.getInt(num - 1)
              val end = interval.end.getInt(num - 1)
              (v.compare(start) > 0 && v.compare(end) < 0) ||
                (interval.startInclude && v.compare(start) == 0) ||
                (interval.endInclude && v.compare(end) == 0)
            case other => sys.error(s"not support data type: $other")
          }
        }.map{r =>
          val key = InternalRow.fromSeq(prefixValues :+ r)
          RangeInterval(key, key, includeStart = true, includeEnd = true)
        }
      }
    } else {
      intervalArray.map{ interval =>
        val start = InternalRow.fromSeq(prefixValues :+
          interval.start.get(num - 1, schema.last.dataType))
        val end = InternalRow.fromSeq(prefixValues :+
          interval.end.get(num - 1, schema.last.dataType))
        RangeInterval(start, end, interval.startInclude, interval.endInclude)
      }
    }
  }
}

/**
 * The data file handle, will be cached for performance purpose, as we don't want to open the
 * specified file again and again to get its data meta, the data file extension can have its own
 * implementation.
 */
abstract class DataFileHandle {
  def fin: FSDataInputStream
  def len: Long
}
