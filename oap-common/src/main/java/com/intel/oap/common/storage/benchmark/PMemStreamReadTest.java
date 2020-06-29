package com.intel.oap.common.storage.benchmark;

import com.intel.oap.common.storage.stream.ChunkInputStream;
import com.intel.oap.common.storage.stream.ChunkOutputStream;
import com.intel.oap.common.storage.stream.DataStore;
import com.intel.oap.common.storage.stream.PMemManager;

import org.openjdk.jmh.annotations.*;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
@Warmup(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class PMemStreamReadTest {
    ChunkOutputStream chunkOutputStream;
    ChunkInputStream chunkInputStream;
    byte[] data;
    byte[] readData = new byte[4*1024*1024];
    DataStore dataStore;
    String fileName = "target/test.file";
    @Setup(Level.Trial)
    public void intializeChunkOutputStream() throws IOException{
        Properties p = new Properties();
        p.setProperty("totalSize", "400000000000");
        p.setProperty("chunkSize", "4194304");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager);
        JmhTestUtil.PMemInitialize();
        data = JmhTestUtil.PMemTestDataParation();
        chunkOutputStream = new ChunkOutputStream(fileName, dataStore);
        chunkOutputStream.write(data);
        chunkOutputStream.close();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void measurePMemStreamRead() throws IOException {
        chunkInputStream = new ChunkInputStream(fileName, dataStore);
        chunkInputStream.read(readData);
        chunkInputStream.close();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PMemStreamReadTest.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}