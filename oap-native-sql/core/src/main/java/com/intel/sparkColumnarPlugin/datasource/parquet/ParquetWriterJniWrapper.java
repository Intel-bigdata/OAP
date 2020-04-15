package com.intel.sparkColumnarPlugin.datasource.parquet;

import com.intel.sparkColumnarPlugin.vectorized.JniUtils;
import java.io.IOException;

/** Wrapper for Parquet Writer native API. */
public class ParquetWriterJniWrapper {

  /** Construct a Jni Instance. */
  public ParquetWriterJniWrapper() throws IOException {
    JniUtils.getInstance();
  }

  /**
   * Construct a parquet file reader over the target file name.
   *
   * @param path absolute file path of target file
   * @param schemaBytes a byte array of Schema serialized output
   * @return long id of the parquet writer instance
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native long nativeOpenParquetWriter(String path, byte[] schemaBytes);

  /**
   * Close a parquet file writer.
   *
   * @param id parquet writer instance number
   */
  public native void nativeCloseParquetWriter(long id);

  /**
   * Write next record batch to parquet file writer.
   *
   * @param id parquet writer instance number
   * @param numRows number of Rows in this batch
   * @param bufAddrs a array of buffers address of this batch
   * @param bufSizes a array of buffers size of this batch
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native void nativeWriteNext(long id, int numRows, long[] bufAddrs, long[] bufSizes);
}
