package org.apache.spark.sql.execution.datasources

import org.apache.parquet.hadoop.api.RecordReader

class FileRecordReaderIterator[ID, V](rowReader: RecordReader[ID, V])
    extends Iterator[V] {
  private[this] var havePair = false
  private[this] var finished = false

  override def hasNext: Boolean = {
    if (!finished && !havePair) {
      finished = !rowReader.nextKeyValue
      if (finished) {
        // Close and release the reader here; close() will also be called when the task
        // completes, but for tasks that read from many files, it helps to release the
        // resources early.
        rowReader.close()
      }
      havePair = !finished
    }
    !finished
  }

  override def next(): V = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    havePair = false
    rowReader.getCurrentValue
  }
}