package org.apache.parquet.hadoop;

import static java.lang.String.format;
import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.PositionableRecordReader;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SpinachMessageColumnIO;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class InternalSpinachRecordReader<T> implements InternalRecordReader<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(InternalSpinachRecordReader.class);

    protected final ColumnIOFactory columnIOFactory = new ColumnIOFactory();
    protected final Filter filter;

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

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     * @param filter for filtering individual records
     */
    public InternalSpinachRecordReader(ReadSupport<T> readSupport, Filter filter) {
        this.readSupport = readSupport;
        this.filter = checkNotNull(filter, "filter");
    }

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     */
    public InternalSpinachRecordReader(ReadSupport<T> readSupport) {
        this(readSupport, FilterCompat.NOOP);
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
            MessageColumnIO columnIO =
                    columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
            SpinachMessageColumnIO spinachMessageColumnIO = new SpinachMessageColumnIO(columnIO);
            RecordReader<T> recordReader =
                    spinachMessageColumnIO.getRecordReader(pages, recordConverter, filter);
            startedAssemblingCurrentBlockAt = System.currentTimeMillis();
            this.pRecordReader = getPositionableRecordReader(recordReader, pages.getRowCount());
            totalCountLoadedSoFar += pRecordReader.getRecordCount();
            ++currentBlock;
        }
    }

    protected abstract PositionableRecordReader<T> getPositionableRecordReader(RecordReader<T> recordReader,
            long rowCount);

    @Override
    public void close() throws IOException {
        if (parquetFileReader != null) {
            parquetFileReader.close();
        }
    }

    @Override
    public void initialize(MessageType fileSchema, Map<String, String> fileMetadata, Path file,
                           List<BlockMetaData> blocks, List<List<Long>> rowIdsList, Configuration configuration)
            throws IOException {
        // initialize a ReadContext for this file
        ReadSupport.ReadContext readContext =
                readSupport.init(new InitContext(configuration, toSetMultiMap(fileMetadata), fileSchema));
        this.requestedSchema = readContext.getRequestedSchema();
        this.fileSchema = fileSchema;
        this.file = file;
        this.columnCount = requestedSchema.getPaths().size();
        this.recordConverter =
                readSupport.prepareForRead(configuration, fileMetadata, fileSchema, readContext);
        this.strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true);
        List<ColumnDescriptor> columns = requestedSchema.getColumns();
        parquetFileReader = new ParquetFileReader(configuration, file, blocks, columns);
        this.initOthers(rowIdsList, blocks);
        LOG.info("RecordReader initialized will read a total of " + total + " records.");
    }

    protected abstract void initOthers(List<List<Long>> rowIdsList, List<BlockMetaData> blocks);

    @Override
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

    @Override
    public Void getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) current / total;
    }

    @Override
    public int getCurrentBlockIndex() {
        return currentBlock;
    }

    @Override
    public long getInternalRowId() {
        return pRecordReader.getCurrentRowId();
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
