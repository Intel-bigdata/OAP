package org.apache.parquet.hadoop;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.execution.datasources.oap.io.OapReadSupportImpl;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport$;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;

public abstract class SpecificOapRecordReaderBase<T> implements RecordReader<T> {

    protected Path file;
    protected MessageType fileSchema;
    protected MessageType requestedSchema;
    protected StructType sparkSchema;
    protected Configuration configuration;
    protected ParquetMetadata footer;

    /**
     * The total number of rows this RecordReader will eventually read. The sum of the
     * rows of all the row groups.
     */
    protected long totalRowCount;

    protected ParquetFileReader reader;

    @Override
    public void initialize() throws IOException, InterruptedException {
        if(this.footer == null){
            footer = readFooter(configuration, file, NO_FILTER);
        }
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        FilterCompat.Filter filter = getFilter(configuration);
        List<BlockMetaData> blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
        this.fileSchema = footer.getFileMetaData().getSchema();

        Map<String, String> fileMetadata = footer.getFileMetaData().getKeyValueMetaData();
        ReadSupport.ReadContext readContext = new OapReadSupportImpl().init(new InitContext(
                configuration, toSetMultiMap(fileMetadata), fileSchema));
        this.requestedSchema = readContext.getRequestedSchema();
        String sparkRequestedSchemaString =
                configuration.get(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA());
        this.sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
        //TODO this api deprecated
        this.reader = new ParquetFileReader(
                configuration, footer.getFileMetaData(), file, blocks, requestedSchema.getColumns());
        for (BlockMetaData block : blocks) {
            this.totalRowCount += block.getRowCount();
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Set<V> set = new HashSet<V>();
            set.add(entry.getValue());
            setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }

}
