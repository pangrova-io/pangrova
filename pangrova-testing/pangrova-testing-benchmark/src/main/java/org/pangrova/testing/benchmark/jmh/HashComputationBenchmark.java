package org.pangrova.testing.benchmark.jmh;

import java.util.concurrent.TimeUnit;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for hash computation used in bloom filters and shuffle partitioning.
 *
 * <p>Measures bloom filter build and probe throughput at various scales.
 * The bloom filter internally uses xxHash64 for hash computation, making this
 * an effective benchmark for the hash hot path in runtime filter push-down
 * and shuffle hash partitioning.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgs = {"--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class HashComputationBenchmark {

    @Param({"1000", "10000", "100000"})
    private int filterSize;

    private SplitBlockBloomFilter bloomFilter;
    private String[] probeValues;
    private String[] insertValues;

    @Setup
    public void setup() {
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(filterSize, 0.01);
        for (int i = 0; i < filterSize; i++) {
            builder.put("value_" + i);
        }
        bloomFilter = builder.build();

        // Prepare probe values: mix of present and absent keys
        probeValues = new String[1000];
        for (int i = 0; i < 1000; i++) {
            probeValues[i] = (i % 2 == 0) ? "value_" + (i * 5) : "missing_" + i;
        }

        // Prepare insert values for build benchmark
        insertValues = new String[1000];
        for (int i = 0; i < 1000; i++) {
            insertValues[i] = "insert_key_" + i;
        }
    }

    @Benchmark
    public void bloomFilterProbe(Blackhole bh) {
        for (String value : probeValues) {
            bh.consume(bloomFilter.mightContain(value));
        }
    }

    @Benchmark
    public void bloomFilterBuild(Blackhole bh) {
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(1000, 0.01);
        for (String value : insertValues) {
            builder.put(value);
        }
        bh.consume(builder.build());
    }

    @Benchmark
    public void bloomFilterSerializeRoundTrip(Blackhole bh) {
        byte[] serialized = bloomFilter.toByteArray();
        bh.consume(SplitBlockBloomFilter.readFrom(serialized));
    }
}
