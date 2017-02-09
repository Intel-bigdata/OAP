package org.apache.parquet.hadoop;

import static java.lang.String.format;
import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
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

    protected static final Logger LOG = LoggerFactory.getLogger(InternalSpinachRecordReader.class);

    protected SColumnIOFactory columnIOFactory;

    protected MessageType requestedSchema;
    protected MessageType fileSchema;
    protected int columnCount;
    protected final ReadSupport<T> readSupport;

    protected RecordMaterializer<T> recordConverter;

    protected T currentValue;
    protected long total;
    protected long current = 0;
    protected int currentBlock = -1;

    protected boolean strictTypeChecking;

    protected long totalTimeSpentReadingBytes;
    protected long totalTimeSpentProcessingRecords;
    protected long startedAssemblingCurrentBlockAt;

    protected long totalCountLoadedSoFar = 0;

    protected Path file;

    protected ParquetFileReader parquetFileReader;

    protected PositionableRecordReader<T> pRecordReader;

    protected Iterator<List<Long>> rowIdsIter = null;

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     */
    public InternalSpinachRecordReader(ReadSupport<T> readSupport) {
        this.readSupport = readSupport;
    }

    protected void checkRead() throws IOException {
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
            PageReadStore pages = parquetFileReader.readNextRowGroup();
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
            RecordReader<T> recordReader = columnIO.getRecordReader(pages,recordConverter);

            startedAssemblingCurrentBlockAt = System.currentTimeMillis();
            this.pRecordReader = getPositionableRecordReader(recordReader, pages.getRowCount());
            totalCountLoadedSoFar += pRecordReader.getRecordCount();
            ++currentBlock;
        }
    }

    protected PositionableRecordReader<T> getPositionableRecordReader(RecordReader<T> recordReader,
                                                                      long rowCount) {
        return new RowIdIteratorRecordReaderImpl<>(recordReader, rowIdsIter.next(), rowCount);
    }

    public void close() throws IOException {
        if (parquetFileReader != null) {
            parquetFileReader.close();
        }
    }

    public void initialize(ParquetFileReader parquetFileReader, Configuration configuration)
            throws IOException {
        this.parquetFileReader = parquetFileReader;
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
        this.parquetFileReader.setRequestedSchema(requestedSchema);
    }

    public void initOthers(List<List<Long>> rowIdsList) {
        this.rowIdsIter = rowIdsList.iterator();
        for (List<Long> rowIdList : rowIdsList) {
            total += rowIdList.size();
        }
        LOG.info("RecordReader initialized will read a total of {} records.", total);
    }


    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean recordFound = false;

        while (!recordFound) {
            // no more records left
            if (current >= total) {
                return false;
            }

            try {
                checkRead();
                this.currentValue = pRecordReader.read();
                current++;
                if (pRecordReader.shouldSkipCurrentRecord()) {
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
                        current, currentBlock, file), e);
            }
        }
        return true;
    }

    public T getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    public float getProgress() throws IOException, InterruptedException {
        if(total == 0L){
            return 1F;
        }
        return (float) current / total;
    }

    private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<K, Set<V>>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Set<V> set = new HashSet<V>();
            set.add(entry.getValue());
            setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }

}
