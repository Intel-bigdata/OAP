package com.intel.oap.common.storage.benchmark;

import com.intel.oap.common.unsafe.PersistentMemoryPlatform;
import com.intel.oap.common.util.MemCopyUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import sun.misc.Unsafe;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class PMemDirectReadTest {
    byte[] data;
    long baseAddr;
    @Setup(Level.Trial)
    public void intializePMem() {
        JmhTestUtil.PMemInitialize();
        data = JmhTestUtil.PMemTestDataParation();
        long length = data.length;
        baseAddr = PersistentMemoryPlatform.allocateVolatileMemory(length);
        MemCopyUtil.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, baseAddr, length);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void measurePMemDirectRead() {
        byte[] readData = new byte[data.length];
        MemCopyUtil.copyMemory(null, baseAddr, readData, Unsafe.ARRAY_BYTE_BASE_OFFSET, data.length);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PMemDirectReadTest.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}