/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.apache.commons.io.IOUtils
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

/**
 * The usage for RoaringBitmap.
 */
class BitmapUsageSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  private var dir: File = _
  private var path: String = _

  override def beforeEach(): Unit = {
    dir = Utils.createTempDir()
    path = dir.getAbsolutePath
  }

  override def afterEach(): Unit = {
    dir.delete()
  }

  test("test how to serialize roaring bitmap to file and deserialize back") {
    val rb1 = new RoaringBitmap()
    val rb2 = new RoaringBitmap()
    val rb3 = new RoaringBitmap()
    (0 until 100000).foreach(rb1.add)
    (100000 until 200000).foreach(element => rb2.add(3 * element))
    (700000 until 800000).foreach(rb3.add)
    val file = path + "roaringbitmaps.bin"
    val out = new DataOutputStream(new FileOutputStream(file))
    val headerLength = 4
    out.writeInt(headerLength)
    rb1.runOptimize()
    rb2.runOptimize()
    rb3.runOptimize()
    rb1.serialize(out)
    rb2.serialize(out)
    rb3.serialize(out)
    out.close()
    // verify:
    val int = new DataInputStream(new FileInputStream(file))
    // The 4 is the four bytes for header length.
    val headerLengthRead = int.readInt()
    int.skip(rb1.serializedSizeInBytes + rb2.serializedSizeInBytes)
    val rbtest3 = new RoaringBitmap()
    rbtest3.deserialize(int)
    if (!rbtest3.equals(rb3)) {
      throw new RuntimeException("bug!")
    }
  }

  test("test to use MutableRoaringBitmap and ImmutableRoarigBitmap " +
    "to serialize to file and deserialize back") {
    val rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000)
    val rr2 = MutableRoaringBitmap.bitmapOf( 2, 3, 1010)
    val file = path + "mutableroaringbitmaps.bin"
    val dos = new DataOutputStream(new FileOutputStream(file))
    val headerLength = 4
    dos.writeInt(headerLength)
    rr1.runOptimize()
    rr2.runOptimize()
    rr1.serialize(dos)
    rr2.serialize(dos)
    dos.close()
    val bb = ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(file)))
    bb.position(4 + rr1.serializedSizeInBytes())
    val rrback2 = new ImmutableRoaringBitmap(bb)
    assert(rrback2 == rr2)
  }

  test("test to use memory map for roaring bitmaps") {
    val tmpfile = File.createTempFile("roaring", "bin")
    tmpfile.deleteOnExit()
    val fos = new FileOutputStream(tmpfile)
    val Bitmap1 = MutableRoaringBitmap.bitmapOf(0, 2, 55, 64, 1 << 30)
    val Bitmap2 = MutableRoaringBitmap.bitmapOf(0, 2, 55, 654, 1 << 35)
    val pos1 = 0 // bitmap 1 is at offset 0
    Bitmap1.runOptimize()
    Bitmap1.serialize(new DataOutputStream(fos))
    val pos2 = Bitmap1.serializedSizeInBytes() // bitmap 2 will be right after it
    Bitmap2.runOptimize()
    Bitmap2.serialize(new DataOutputStream(fos))
    val totalcount = fos.getChannel.position()
    if (totalcount != Bitmap1.serializedSizeInBytes() + Bitmap2.serializedSizeInBytes()) {
      throw new RuntimeException("This will not happen.")
    }
    fos.close()
    val memoryMappedFile = new RandomAccessFile(tmpfile, "r")
    val bb = memoryMappedFile.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, totalcount)
    memoryMappedFile.close() // we can safely close
    bb.position(pos1)
    val mapped1 = new ImmutableRoaringBitmap(bb)
    bb.position(pos2)
    val mapped2 = new ImmutableRoaringBitmap(bb)
    assert(mapped1 == Bitmap1)
    assert(mapped2 == Bitmap2)
  }
}
