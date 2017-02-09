package org.apache.parquet.hadoop;

import static java.lang.String.format;
import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.*;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalSpinachRecordReader<T> {

    private static final Logger LOG = LoggerFactory.getLogger(InternalSpinachRecordReader.class);

    private SColumnIOFactory columnIOFactory;

    private MessageType requestedSchema;
    private MessageType fileSchema;
    private int columnCount;
    private final ReadSupport<T> readSupport;

    private RecordMaterializer<T> recordConverter;

    private T currentValue;
    private long total;
    private long current = 0;
    private int currentBlock = -1;

    private boolean strictTypeChecking;

    private long totalTimeSpentReadingBytes;
    private long totalTimeSpentProcessingRecords;
    private long startedAssemblingCurrentBlockAt;

    private long totalCountLoadedSoFar = 0;

    private ParquetFileReader reader;

    private RecordReader<T> recordReader;

    private Iterator<List<Long>> rowIdsIter = null;

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     */
    InternalSpinachRecordReader(ReadSupport<T> readSupport) {
        this.readSupport = readSupport;
    }

    private void checkRead() throws IOException {
        if (current == totalCountLoadedSoFar) {
            if (current != 0) {
                totalTimeSpentProcessingRecords
                        += (System.currentTimeMillis() - startedAssemblingCurrentBlockAt);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Assembled and processed " + totalCountLoadedSoFar + " records from "
                            + columnCount + " columns in " + totalTimeSpentProcessingRecords + " ms: "
                            + ((float) totalCountLoadedSoFar / totalTimeSpentProcessingRecords) + " rec/ms, "
                            + ((float) totalCountLoadedSoFar * columnCount / totalTimeSpentProcessingRecords)
                            + " cell/ms");
                    final long totalTime = totalTimeSpentProcessingRecords + totalTimeSpentReadingBytes;
                    if (totalTime != 0) {
                        final long percentReading = 100 * totalTimeSpentReadingBytes / totalTime;
                        final long percentProcessing = 100 * totalTimeSpentProcessingRecords / totalTime;
                        LOG.info("time spent so far " + percentReading + "% reading ("
                                + totalTimeSpentReadingBytes + " ms) and " + percentProcessing
                                + "% processing (" + totalTimeSpentProcessingRecords + " ms)");
                    }
                }
            }

            LOG.info("at row " + current + ". reading next block");
            long t0 = System.currentTimeMillis();
            PageReadStore pages = reader.readNextRowGroup();
            if (pages == null) {
                throw new IOException(
                        "expecting more rows but reached last block. Read " + current + " out of " + total);
            }

            long timeSpentReading = System.currentTimeMillis() - t0;
            totalTimeSpentReadingBytes += timeSpentReading;
            BenchmarkCounter.incrementTime(timeSpentReading);
            if (LOG.isInfoEnabled()) {
                LOG.info("block read in memory in {} ms. row count = {}",
                        timeSpentReading, pages.getRowCount());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("initializing Record assembly with requested schema {}", requestedSchema);
            }
            SColumnMessageIO columnIO =
                    columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
            List<Long> rowIdList = rowIdsIter.next();
            this.recordReader = columnIO.getRecordReader(pages, recordConverter, rowIdList);
            startedAssemblingCurrentBlockAt = System.currentTimeMillis();
            totalCountLoadedSoFar += rowIdList.size();
            ++currentBlock;
        }
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    public void initialize(ParquetFileReader parquetFileReader, Configuration configuration)
            throws IOException {
        this.reader = parquetFileReader;
        FileMetaData parquetFileMetadata = parquetFileReader.getFooter().getFileMetaData();
        this.fileSchema = parquetFileMetadata.getSchema();
        Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
        ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
                configuration, toSetMultiMap(fileMetadata), fileSchema));
        this.columnIOFactory = new SColumnIOFactory(parquetFileMetadata.getCreatedBy());
        this.requestedSchema = readContext.getRequestedSchema();
        this.columnCount = requestedSchema.getPaths().size();
        this.recordConverter = readSupport.prepareForRead(
                configuration, fileMetadata, fileSchema, readContext);
        this.strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true);
//        this.unmaterializableRecordCounter = new UnmaterializableRecordCounter(configuration, total);
//        this.filterRecords = configuration.getBoolean(
//                RECORD_FILTERING_ENABLED, RECORD_FILTERING_ENABLED_DEFAULT);

        //TODO init total count
        this.reader.setRequestedSchema(requestedSchema);
    }

    void initOthers(List<List<Long>> rowIdsList) {
        this.rowIdsIter = rowIdsList.iterator();
        for (List<Long> rowIdList : rowIdsList) {
            total += rowIdList.size();
        }
        LOG.info("RecordReader initialized will read a total of {} records.", total);
    }


    boolean nextKeyValue() throws IOException, InterruptedException {
        boolean recordFound = false;

        while (!recordFound) {
            // no more records left
            if (current >= total) {
                return false;
            }

            try {
                checkRead();
                this.currentValue = recordReader.read();
                current++;
                if (recordReader.shouldSkipCurrentRecord()) {
                    // this record is being filtered via the filter2 package
                    if (DEBUG) {
                        LOG.debug("skipping record");
                    }
                    continue;
                }

                recordFound = true;

                if (DEBUG) {
                    LOG.debug("read value: " + currentValue);
                }
            } catch (RuntimeException e) {
                throw new ParquetDecodingException(format("Can not read value at %d in block %d in file %s",
                        current, currentBlock, reader.getPath()), e);
            }
        }
        return true;
    }

    T getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    public float getProgress() throws IOException, InterruptedException {
        if(total == 0L){
            return 1F;
        }
        return (float) current / total;
    }

    private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Set<V> set = new HashSet<>();
            set.add(entry.getValue());
            setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }

}
