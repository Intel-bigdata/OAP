package com.intel.sparkColumnarPlugin.datasource.parquet;

import com.intel.sparkColumnarPlugin.vectorized.ArrowRecordBatchBuilder;
import com.intel.sparkColumnarPlugin.vectorized.JniUtils;

import java.io.IOException;


/** Wrapper for Parquet Reader native API. */
public class ParquetReaderJniWrapper {

  /** Construct a Jni Instance. */
  ParquetReaderJniWrapper() throws IOException {
    JniUtils.getInstance();
  }

  /**
   * Construct a parquet file reader over the target file name.
   *
   * @param path absolute file path of target file
   * @param batchSize number of rows of one readed batch
   * @return long id of the parquet reader instance
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native long nativeOpenParquetReader(String path, long batchSize) throws IOException;

  /**
   * Init a parquet file reader by specifying columns and rowgroups.
   *
   * @param id parquet reader instance number
   * @param columnIndices a array of indexes indicate which columns to be read
   * @param rowGroupIndices a array of indexes indicate which row groups to be read
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native void nativeInitParquetReader(long id, int[] columnIndices, int[] rowGroupIndices)
      throws IOException;

  /**
   * Init a parquet file reader by specifying columns and rowgroups.
   *
   * @param id parquet reader instance number
   * @param columnIndices a array of indexes indicate which columns to be read
   * @param startPos a start pos to indicate which row group to be read
   * @param endPos a end pos to indicate which row group to be read
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native void nativeInitParquetReader2(
      long id, int[] columnIndices, long startPos, long endPos) throws IOException;

  /**
   * Close a parquet file reader.
   *
   * @param id parquet reader instance number
   */
  public native void nativeCloseParquetReader(long id);

  /**
   * Read next record batch from parquet file reader.
   *
   * @param id parquet reader instance number
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native ArrowRecordBatchBuilder nativeReadNext(long id) throws IOException;

  /**
   * Get schema from parquet file reader.
   *
   * @param id parquet reader instance number
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native byte[] nativeGetSchema(long id) throws IOException;
}
