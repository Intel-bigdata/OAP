package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

public class ColumnVectorAllocator {

  public static ColumnVector[] allocateColumns(
      MemoryMode memoryMode, int capacity, StructType schema) {
    if (memoryMode == MemoryMode.OFF_HEAP) {
      return OffHeapColumnVector.allocateColumns(capacity, schema);
    } else {
      return OapOnHeapColumnVector.allocateColumns(capacity, schema);
    }
  }
}
