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

import java.io.{ByteArrayOutputStream, DataOutputStream, OutputStream}

import scala.collection.immutable
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.FromUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsWriteManager
import org.apache.spark.sql.execution.datasources.oap.utils.NonNullKeyWriter
import org.apache.spark.sql.types._

private[oap] object BitmapIndexSectionId {
  val headerSection : Int = 1 // header
  val keyListSection : Int = 2 // sorted unique key list (index column unique values)
  val entryListSection : Int = 3 // bitmap entry list
  val entryNullSection : Int = 4 // bitmap entry for null value rows
  val entryOffsetsSection : Int = 5 // bitmap entry offset list
  val statsContentSection : Int = 6 // keep the original statistics, not changed than before.
  val footerSection : Int = 7 // footer to save the meta data for the above segments.
}

/* Below is the bitmap index general layout and sections.
 * #section                size(B)       description
 * headerSection           4             header
 * keyListSection          varied        sorted unique key list (index column unique values)
 * entryListSection        varied        bitmap entry list --
 *                                       Null key's entries are appended to the end
 * entryOffsetSection      varied        bitmap entry offset list
 * statsContentSection     varied        keep the original statistics, not changed than before.
 * footerSection           44(4 + 5*8)   save total key list size and length, total entry list
 *                                       size, total offset list size, and stats meta data.
 * Below is each entry description within footerSection.
 * Index version number    4
 * Non-null key List Size  4
 * Non-Null key count      4
 * Non-null key Entry Size 4
 * Offset Section Size     4
 * Null key entries offset 4
 * Null key entry size     4
 * stats offset position   8
 * stats size              8
 *
 * TODO: 1. Bitmap index is suitable for those columns which actually has not many
 *          unique values, thus we will load the key list and offset list respectively as a
 *          whole, rather than splitting into more smaller partial loading units.
 *       2. Bitmap index query will be limited for equal queries, thus each equal query will
 *          only be corresponding to one single bitmap entry. It's not decided yet to regard
 *          and cache each targeted bitmap entry or several consecutive bitmap entries as the
 *          partialy loading unit. It will depend on the following benchmark profiling result.
 */
private[oap] class BitmapIndexRecordWriter(
    configuration: Configuration,
    writer: OutputStream,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)
  @transient private lazy val nnkw = new NonNullKeyWriter(keySchema)

  private val rowMapBitmap = new mutable.HashMap[InternalRow, RoaringBitmap]()
  private var recordCount: Int = 0
  private lazy val statisticsWriteManager = new StatisticsWriteManager {
    this.initialize(BitMapIndexType, keySchema, configuration)
  }

  private var bmEntryListOffset: Int = _

  private var bmUniqueKeyList: immutable.List[InternalRow] = _
  private var bmNullKeyList: immutable.List[InternalRow] = _
  private var bmOffsetListBuffer: mutable.ListBuffer[Int] = _
  private var bmNullEntryOffset: Int = _
  private var bmNullEntrySize: Int = _

  private var bmUniqueKeyListTotalSize: Int = _
  private var bmUniqueKeyListCount: Int = _
  private var bmEntryListTotalSize: Int = _
  private var bmOffsetListTotalSize: Int = _
  private var bmIndexEnd: Int = _

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    if (!rowMapBitmap.contains(v)) {
      val bm = new RoaringBitmap()
      bm.add(recordCount)
      rowMapBitmap.put(v, bm)
      statisticsWriteManager.addOapKey(v)
    } else {
      rowMapBitmap.get(v).get.add(recordCount)
    }
    if (recordCount == Int.MaxValue) {
      throw new OapException("Cannot support indexing more than 2G rows!")
    }
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    flushToFile()
    writer.close()
  }

  private def writeUniqueKeyList(): Unit = {
    val ordering = GenerateOrdering.create(keySchema)
    // val (bmNullKeyList, bmUniqueKeyList) =
    val (nullKeyList, uniqueKeyList) = rowMapBitmap.keySet.toList.partition(_.anyNull)
    bmNullKeyList = nullKeyList
    assert(bmNullKeyList.size <= 1) // At most one null key exists.
    bmUniqueKeyList = uniqueKeyList.sorted(ordering)
    val bos = new ByteArrayOutputStream()
    bmUniqueKeyList.foreach(key => nnkw.writeKey(bos, key))
    bmUniqueKeyListTotalSize = bos.size()
    bmUniqueKeyListCount = bmUniqueKeyList.size
    writer.write(bos.toByteArray)
  }

  private def writeBmEntryList(): Unit = {
    /* TODO: 1. Use BitSet of Spark or Scala or Java to replace roaring bitmap.
     *       2. Optimize roaring bitmap usage to further reduce index file size.
     */
    bmEntryListOffset = IndexFile.VERSION_LENGTH + bmUniqueKeyListTotalSize
    bmOffsetListBuffer = new mutable.ListBuffer[Int]()
    // Get the bitmap total size, and write each bitmap entrys one by one.
    var totalBitmapSize = 0
    bmUniqueKeyList.foreach(uniqueKey => {
      bmOffsetListBuffer.append(bmEntryListOffset + totalBitmapSize)
      val bm = rowMapBitmap.get(uniqueKey).get
      bm.runOptimize()
      val bos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(bos)
      // Below serialization is directly written into byte/int/long arrays
      // according to roaring bitmap internal format(http://roaringbitmap.org/).
      bm.serialize(dos)
      dos.flush()
      totalBitmapSize += bos.size
      writer.write(bos.toByteArray)
      dos.close()
      bos.close()
    })
    bmEntryListTotalSize = totalBitmapSize

    // Write entry for null value rows if exists
    if (bmNullKeyList.nonEmpty) {
      bmNullEntryOffset = bmEntryListOffset + totalBitmapSize
      val bm = rowMapBitmap.get(bmNullKeyList.head).get
      bm.runOptimize()
      val bos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(bos)
      bm.serialize(dos)
      dos.flush()
      bmNullEntrySize = bos.size
      writer.write(bos.toByteArray)
      dos.close()
      bos.close()
    }
  }

  private def writeBmOffsetList(): Unit = {
    // write offset for each bitmap entry to fast locate expected bitmap entries during scanning.
    bmOffsetListBuffer.foreach(offsetIdx => IndexUtils.writeInt(writer, offsetIdx))
    bmOffsetListTotalSize = 4 * bmOffsetListBuffer.size
  }

  private def writeBmFooter(): Unit = {
    // The beginning of the footer are bitmap total size, key list size and offset total size.
    // Others keep back compatible and not changed than before.
    bmIndexEnd = bmEntryListOffset + bmEntryListTotalSize + bmNullEntrySize + bmOffsetListTotalSize
    // The index end is also the starting position of statistics part.
    val statSize = statisticsWriteManager.write(writer)
    IndexUtils.writeInt(writer, IndexFile.VERSION_NUM)
    IndexUtils.writeInt(writer, bmUniqueKeyListTotalSize)
    IndexUtils.writeInt(writer, bmUniqueKeyListCount)
    IndexUtils.writeInt(writer, bmEntryListTotalSize)
    IndexUtils.writeInt(writer, bmOffsetListTotalSize)
    IndexUtils.writeInt(writer, bmNullEntryOffset)
    IndexUtils.writeInt(writer, bmNullEntrySize)
    IndexUtils.writeLong(writer, bmIndexEnd)
    IndexUtils.writeLong(writer, statSize.toLong)
  }

  private def flushToFile(): Unit = {
    IndexUtils.writeHead(writer, IndexFile.VERSION_NUM)
    writeUniqueKeyList()
    writeBmEntryList()
    writeBmOffsetList()
    writeBmFooter()
  }
}
