package com.intel.oap.common.storage.benchmark;

import com.intel.oap.common.storage.stream.ChunkOutputStream;
import com.intel.oap.common.storage.stream.DataStore;
import com.intel.oap.common.storage.stream.PMemManager;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
public class PMemStreamWriteTest {
    ChunkOutputStream chunkOutputStream;
    byte[] data;
    DataStore dataStore;
    @Setup(Level.Trial)
    public void intializeChunkOutputStream() {
        Properties p = new Properties();
        p.setProperty("totalSize", "400000000000");
        p.setProperty("chunkSize", "4194304");
        PMemManager pMemManager = new PMemManager(p);
        dataStore = new DataStore(pMemManager);
        JmhTestUtil.PMemInitialize();
        data = JmhTestUtil.PMemTestDataParation();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void measurePMemStreamWrite() throws IOException {
        String fileName = "target/test_" + new Timestamp(System.currentTimeMillis()).toString() + ".file";
        chunkOutputStream = new ChunkOutputStream(fileName, dataStore);
        chunkOutputStream.write(data);
        chunkOutputStream.close();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PMemStreamWriteTest.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}