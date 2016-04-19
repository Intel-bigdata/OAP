
package org.apache.spark.sql.execution.datasources.spinach

import java.io.DataOutputStream
import org.apache.hadoop.fs.FSDataInputStream

import scala.collection.mutable.ArrayBuffer

/**
  * Created by shimingfei on 4/15/16.
  */
class SpinachSplitMeta(val rowGroupMeta: Array[(Long, Array[Int])], val rowGroupSize: (Int, Int),
                       val colNum: Int) {

  def write(os: DataOutputStream): Unit = {
    for (meta <- rowGroupMeta) {
      os.writeLong(meta._1)
      for(len <- meta._2) {
        os.writeInt(len)
      }
    }

    os.writeInt(rowGroupMeta.length)
    os.writeInt(rowGroupSize._1)
    os.writeInt(rowGroupSize._2)
    os.writeInt(colNum)
  }
}

/**
  * Used to build SpinachSplitMeta with info provided
  */
class SpinachSplitMetaBuilder() {
  var columnNum: Int = 0
  var defaultRowGroupSize: Int = 0
  var lastRowGroupSize: Int = 0
  val rowGroupMeta: ArrayBuffer[(Long, Array[Int])] = new ArrayBuffer[(Long, Array[Int])]()

  def addRowGroupMeta(rowGroupMeta: (Long, Array[Int])): Unit = {
    this.rowGroupMeta += rowGroupMeta
  }

  def setColumnNumber(columnNum: Int): Unit = {
    this.columnNum = columnNum
  }
  def setDefaultRowGroupSize(rowGroupSize: Int): Unit = {
    this.defaultRowGroupSize = rowGroupSize
  }

  def setLastRowGroupSize(rowGroupSize: Int): Unit = {
    this.lastRowGroupSize = rowGroupSize
  }

  def build(): SpinachSplitMeta = {
    return new SpinachSplitMeta(rowGroupMeta.toArray, (defaultRowGroupSize, lastRowGroupSize),
      columnNum)
  }

  def clear(): Unit = {
    columnNum = 0
    defaultRowGroupSize = 0
    lastRowGroupSize = 0
    rowGroupMeta.clear()
  }
}

object SpinachSplitMetaBuilder {

  def apply(is: FSDataInputStream, fileLen: Long): SpinachSplitMeta = {
    val oldPos = is.getPos
    val builder = new SpinachSplitMetaBuilder()

    is.seek(fileLen - 16L) // the length info
    val groupCount = is.readInt()
    builder.setDefaultRowGroupSize(is.readInt())
    builder.setLastRowGroupSize(is.readInt())

    val columnNum = is.readInt()
    builder.setColumnNumber(columnNum)

    is.seek(fileLen - 16  - 8 * groupCount - 4 * groupCount * columnNum)
    var i = 0
    while(i < groupCount) {
      val startPos = is.readInt()
      val fiberLen = new Array[Int](columnNum)
      var idx = 0
      while(idx < columnNum) {
        fiberLen(idx) = is.readInt()
        idx += 1
      }
      builder.addRowGroupMeta((startPos, fiberLen))
      i += 1
    }
    is.seek(oldPos)

    return builder.build()
  }
}
