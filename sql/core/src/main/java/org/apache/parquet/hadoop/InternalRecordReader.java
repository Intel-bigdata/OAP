package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public interface InternalRecordReader<T> {
    void close() throws IOException;

    void initialize(MessageType fileSchema, Map<String, String> fileMetadata, Path file,
                    List<BlockMetaData> blocks, List<List<Long>> rowIdsList, Configuration configuration)
            throws IOException;

    boolean nextKeyValue() throws IOException, InterruptedException;

    Void getCurrentKey() throws IOException, InterruptedException;

    T getCurrentValue() throws IOException, InterruptedException;

    float getProgress() throws IOException, InterruptedException;

    int getCurrentBlockIndex();

    long getInternalRowId();
}
