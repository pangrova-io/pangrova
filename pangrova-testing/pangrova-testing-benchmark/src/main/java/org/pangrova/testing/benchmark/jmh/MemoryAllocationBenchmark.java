package org.pangrova.testing.benchmark.jmh;

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for memory allocation patterns in the query engine.
 *
 * <p>Measures the cost of Arrow buffer allocation and deallocation at various sizes,
 * simulating the memory allocation patterns seen during query execution. Off-heap
 * memory management is a critical hot path that directly impacts query latency.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgs = {"--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class MemoryAllocationBenchmark {

    @Param({"1024", "4096", "16384"})
    private int batchSize;

    private BufferAllocator allocator;

    @Setup(Level.Trial)
    public void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        allocator.close();
    }

    @Benchmark
    public void allocateAndFillIntVector(Blackhole bh) {
        try (IntVector vector = new IntVector("bench_int", allocator)) {
            vector.allocateNew(batchSize);
            for (int i = 0; i < batchSize; i++) {
                vector.setSafe(i, i * 7);
            }
            vector.setValueCount(batchSize);
            bh.consume(vector.getValueCount());
        }
    }

    @Benchmark
    public void allocateAndFillVarCharVector(Blackhole bh) {
        try (VarCharVector vector = new VarCharVector("bench_varchar", allocator)) {
            vector.allocateNew(batchSize);
            byte[] value = "benchmark_value_data".getBytes();
            for (int i = 0; i < batchSize; i++) {
                vector.setSafe(i, value);
            }
            vector.setValueCount(batchSize);
            bh.consume(vector.getValueCount());
        }
    }

    @Benchmark
    public void allocateAndCloseRapidCycle(Blackhole bh) {
        try (IntVector vector = new IntVector("rapid_cycle", allocator)) {
            vector.allocateNew(batchSize);
            vector.setValueCount(batchSize);
            bh.consume(vector.getBufferSize());
        }
    }
}
