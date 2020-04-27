package org.apache.spark.shuffle.sort;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.*;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.spark.shuffle.remote.RemoteShuffleManager$;
import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.remote.RemoteShuffleBlockResolver;
import org.apache.spark.shuffle.remote.RemoteShuffleManager;
import org.apache.spark.shuffle.remote.RemoteShuffleUtils;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;

@Private
public class RemoteUnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger =
      LoggerFactory.getLogger(RemoteUnsafeShuffleWriter.class);

  static {
    logger.warn("******** Optimized Remote Shuffle Writer is used ********");
  }

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final RemoteShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;
  private final int outputBufferSizeInBytes;

  @Nullable private MapStatus mapStatus;
  @Nullable private RemoteUnsafeShuffleSorter sorter;
  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  private class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }

  public RemoteUnsafeShuffleWriter(
      BlockManager blockManager,
      RemoteShuffleBlockResolver shuffleBlockResolver,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf) throws IOException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
          "RemoteUnsafeShuffleWriter can only be used for shuffles with at most " +
              SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
              " reduce partitions");
    }
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize",
        DEFAULT_INITIAL_SORT_BUFFER_SIZE);
    this.inputBufferSizeInBytes =
        (int) (long) sparkConf
                .get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.outputBufferSizeInBytes =
        (int) (long) sparkConf
                .get(package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      closeAndWriteOutput();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter = new RemoteUnsafeShuffleSorter(
        memoryManager,
        blockManager,
        taskContext,
        shuffleBlockResolver,
        initialSortBufferSize,
        partitioner.numPartitions(),
        sparkConf,
        writeMetrics);
    serBuffer = new RemoteUnsafeShuffleWriter
            .MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    final RemoteSpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    final long[] partitionLengths;
    final Path output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    final Path tmp = RemoteShuffleUtils.tempPathWith(output);
    FileSystem fs = RemoteShuffleManager.getFileSystem();
    try {
      try {
        partitionLengths = mergeSpills(spills, tmp);
      } finally {

        for (RemoteSpillInfo spill : spills) {
          if (fs.exists(spill.file) && ! fs.delete(spill.file, true)) {
            logger.error("Error while deleting spill file {}", spill.file.toString());
          }
        }
      }
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (fs.exists(tmp) && !fs.delete(tmp, true)) {
        logger.error("Error while deleting temp file {}", tmp.toString());
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(
        RemoteShuffleManager$.MODULE$.getResolver().shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);
    serBuffer.reset();
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    sorter.insertRecord(
        serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(RemoteSpillInfo[] spills, Path outputFile) throws IOException {
    final FileSystem fs = RemoteShuffleManager.getFileSystem();
    final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    final boolean fastMergeEnabled =
        sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);
    final boolean fastMergeIsSupported = !compressionEnabled ||
        CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    try {
      if (spills.length == 0) {
        fs.create(outputFile).close(); // Create an empty file
        return new long[partitioner.numPartitions()];
      } else if (spills.length == 1) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        fs.rename(spills[0].file, outputFile);
        return spills[0].partitionLengths;
      } else {
        final long[] partitionLengths;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.

        // We do not perform a transferTo-optimized merge due to underground storage may not support
        // this (NIO FileChannel.transferTo)
        if (fastMergeEnabled && fastMergeIsSupported) {
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          logger.debug("Using fileStream-based fast merge");
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
        } else {
          logger.debug("Using slow merge");
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
        }
        // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
        // in-memory records, we write out the in-memory records to a file but do not count that
        // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
        // to be counted as shuffle write, but this will lead to double-counting of the final
        // RemoteSpillInfo's bytes.
        writeMetrics.decBytesWritten(fs.getFileStatus(spills[spills.length - 1].file).getLen());
        writeMetrics.incBytesWritten(fs.getFileStatus(outputFile).getLen());
        return partitionLengths;
      }
    } catch (IOException e) {
      if (fs.exists(outputFile) && !fs.delete(outputFile, true)) {
        logger.error("Unable to delete output file {}", outputFile.toString());
      }
      throw e;
    }
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than
   * the NIO-based merge,
   * {@link RemoteUnsafeShuffleWriter#mergeSpillsWithTransferTo(RemoteSpillInfo[],
   * Path)}, and it's mostly used in cases where the IO compression codec does not support
   * concatenation of compressed data, when encryption is enabled, or when users have
   * explicitly disabled use of {@code transferTo} in order to work around kernel bugs.
   * This code path might also be faster in cases where individual partition size in a spill
   * is small and RemoteUnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small
   * disk ios which is inefficient. In those case, Using large buffers for input and output
   * files helps reducing the number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithFileStream(
      RemoteSpillInfo[] spills,
      Path outputFile,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    assert (spills.length >= 2);
    final FileSystem fs = RemoteShuffleManager.getFileSystem();
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    final OutputStream bos = new BufferedOutputStream(
        fs.create(outputFile),
        outputBufferSizeInBytes);
    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        // Note by Chenzhao: Originally NioBufferedFileInputStream is used
        spillInputStreams[i] = new BufferedInputStream(
            fs.open(spills[i].file),
            inputBufferSizeInBytes);
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() and flush() calls, so that
        // we can close the higher level streams to make sure all data is really flushed
        // and internal state is cleaned.
        OutputStream partitionOutput =
                new RemoteUnsafeShuffleWriter.CloseAndFlushShieldOutputStream(
            new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));
        partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
        if (compressionCodec != null) {
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) {
            InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i],
                partitionLengthInSpill, false);
            try {
              partitionInputStream = blockManager.serializerManager().wrapForEncryption(
                  partitionInputStream);
              if (compressionCodec != null) {
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              partitionInputStream.close();
            }
          }
        }
        partitionOutput.flush();
        partitionOutput.close();
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      Closeables.close(mergedFileOutputStream, threwException);
    }
    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        try {
          sorter.cleanupResources();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
