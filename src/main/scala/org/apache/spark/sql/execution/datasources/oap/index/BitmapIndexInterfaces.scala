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

package org.apache.spark.sql.execution.datasources.oap.index

private[oap] abstract class BitmapIndexInterfaces {

  // Below three interfaces will be implemented first.
  def BitmapIndexSaveSortedKeySet
  // TODO: The sorted keyset can be cached either in fibers(DRAM)
  // or NVRAM if no rooms in fiber caches.
  def BitmapIndexGetSortedKeySet
  // The sorted keyset require to be updated accordingly once the index is updated or refreshed.
  def BitmapIndexUpdateSortedKeySet

  def BitmapIndexOpenFile
  def BitmapIndexCloseFile

  // Below is to generate hash maps
  def BitmapIndexBuild
  // Below is to scan the hash maps to get the desired bitmap rows.
  def BitmapIndexSearch

  // Below are used to read bitmap sets from hash maps.
  def BitmapIndexReadRows
  def BitmapIndexReadRow
  def BitmapIndexWriteRows
  def BitmapIndexWriteRow

  // Below are used to cache bitmap index data to fibers.
  def BitmapIndexGetFromFibers
  def BitmapIndexSaveToFibers
  // If the index is updated or refreshed, the index fibers will be flushed accordingly.
  def BitmapIndexFlushFibers

  // Below are used to compress/decompress the entire index files.
  def BitmapIndexCompress
  def BitmapIndexDecompress

  // Below are used to encode/decode the single cell or rows or columns.
  def BitmapIndexEncodeBatch
  def BitmapIndexEncodeCell
  def BitmapIndexDecodeBatch
  def BitmapIndexDecodeCell

  // The consistency tool will check index header, index data and index bitmaps.
  def BitmapIndexConsistencyCheck
  // Below is to dump the index file in a friendly approach.
  def BitmapIndexDump

  // TODO: make the bitmap index interfaces are general for different bitmap index implementations,
  // including ours and existing third-party.
}
