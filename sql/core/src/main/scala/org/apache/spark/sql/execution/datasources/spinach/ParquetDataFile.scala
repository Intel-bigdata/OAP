package org.apache.spark.sql.execution.datasources.spinach
import java.lang.{Long => JLong}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.FileRecordReaderIterator
import org.apache.spark.sql.types.StructType
import org.apache.parquet.hadoop.SpinachRecordReader
import org.apache.parquet.hadoop.SpinachRecordReader.Builder
import org.apache.parquet.hadoop.api.RecordReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
  * Created by YangJie on 2016/11/2.
  */
private[spinach] case class ParquetDataFile (path: String, schema: StructType) extends DataFile{

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): FiberCacheData = {null}

  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[InternalRow] = {
      iterator(conf,requiredIds,null)
  }
  def iterator(conf: Configuration, requiredIds: Array[Int], rowIds: Array[Long]): Iterator[InternalRow] = {
    val requestSchemaString = {
      var requestSchema = new StructType
      for(index <- requiredIds) {
        requestSchema = requestSchema.add(schema(index))
      }
      requestSchema.json
    }
    conf.set(SpinachReadSupportImpl.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaString)
    conf.set(SpinachReadSupportImpl.SPARK_ROW_READ_FROM_FILE_SCHEMA, requestSchemaString)

    val readSupport = new SpinachReadSupportImpl

    val recordReader = SpinachRecordReader.builder(readSupport, new Path(path), conf)
      .withGlobalRowIds(rowIds).build()
    recordReader.initialize()
    new FileRecordReaderIterator[JLong, UnsafeRow](
      recordReader.asInstanceOf[RecordReader[JLong, UnsafeRow]])
  }

}
