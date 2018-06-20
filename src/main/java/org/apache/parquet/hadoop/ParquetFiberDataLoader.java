package org.apache.parquet.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.utils.Collections3;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.datasources.oap.io.FiberUsable;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportWrapper;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReaderWrapper;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.OapOnHeapColumnVectorFiber;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;

public class ParquetFiberDataLoader implements Closeable {

  private final int blockId;
  private final int rowGroupCount;

  private final Configuration configuration;
  private final ParquetFiberDataReader reader;
  private OapOnHeapColumnVectorFiber fiber;

  public ParquetFiberDataLoader(
      Configuration configuration,
      ParquetFiberDataReader reader,
      int blockId,
      int rowGroupCount) {
    this.configuration = configuration;
    this.reader = reader;
    this.blockId = blockId;
    this.rowGroupCount = rowGroupCount;
  }

  public FiberUsable load() throws IOException {
    ParquetMetadata footer = reader.getFooter();
    MessageType fileSchema = footer.getFileMetaData().getSchema();
    Map<String, String> fileMetadata = footer.getFileMetaData().getKeyValueMetaData();
    ReadSupport.ReadContext readContext = new ParquetReadSupportWrapper().init(new InitContext(
            configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema));
    MessageType requestedSchema = readContext.getRequestedSchema();
    String sparkRequestedSchemaString =
            configuration.get(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA());
    StructType sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
    boolean isMissing = isMissingColumn(fileSchema, requestedSchema);

    DataType dataType = sparkSchema.fields()[0].dataType();
    ColumnVector vector = ColumnVector.allocate(rowGroupCount, dataType, MemoryMode.ON_HEAP);
    this.fiber =
      new OapOnHeapColumnVectorFiber((OnHeapColumnVector) vector, rowGroupCount, dataType);

    if(isMissing) {
      vector.putNulls(0, rowGroupCount);
      vector.setIsConstant();
    } else {
      // add assert requestedSchema.getColumns().size must 1.
      ColumnDescriptor columnDescriptor = requestedSchema.getColumns().get(0);
      BlockMetaData blockMetaData = footer.getBlocks().get(blockId);
      PageReadStore pageReadStore = reader.readFiberData(blockMetaData, columnDescriptor);
      VectorizedColumnReaderWrapper columnReader = new VectorizedColumnReaderWrapper(
              new VectorizedColumnReader(columnDescriptor,
                      pageReadStore.getPageReader(columnDescriptor)));
      columnReader.readBatch(rowGroupCount, vector);
    }

    return fiber;
  }

  private boolean isMissingColumn(
      MessageType fileSchema,
      MessageType requestedSchema) throws IOException, UnsupportedOperationException {
    Type type = requestedSchema.getFields().get(0);
    if (!type.isPrimitive() || type.isRepetition(Type.Repetition.REPEATED)) {
      throw new UnsupportedOperationException(
              "Complex types " + type.getName() + " not supported.");
    }
    String[] colPath = requestedSchema.getPaths().get(0);
    if (fileSchema.containsPath(colPath)) {
      ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
      if (!fd.equals(requestedSchema.getColumns().get(0))) {
        throw new UnsupportedOperationException("Schema evolution not supported.");
      }
      return false;
    } else {
      if (requestedSchema.getColumns().get(0).getMaxDefinitionLevel() == 0) {
        throw new IOException("Required column is missing in data file. Col: " +
                Arrays.toString(colPath));
      }
      return true;
    }
  }

  @Override
  public void close() throws IOException {
    if (fiber != null) {
      fiber.close();
      fiber = null;
    }
  }

  public void closeWithReader() throws IOException {
    this.close();
    if (reader != null) {
      reader.close();
    }
  }
}
