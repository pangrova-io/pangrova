package org.pangrova.testing.benchmark;

import java.util.Objects;

/**
 * Lightweight benchmark runner for query performance measurement.
 */
public final class PerformanceBenchmark {

    public record BenchmarkConfig(
        String queryName,
        int warmupIterations,
        int measurementIterations) {}

    public record BenchmarkResult(
        String queryName,
        double avgLatencyMs,
        double p99LatencyMs,
        long throughputOpsPerSec) {}

    @FunctionalInterface
    public interface QueryExecutor {
        void execute();
    }

    private PerformanceBenchmark() {}

    public static BenchmarkResult run(BenchmarkConfig config, QueryExecutor executor) {
        Objects.requireNonNull(config);
        Objects.requireNonNull(executor);
        runWarmup(config.warmupIterations(), executor);
        long[] latencies = runMeasurement(config.measurementIterations(), executor);
        return buildResult(config.queryName(), latencies);
    }

    private static void runWarmup(int iterations, QueryExecutor executor) {
        for (int i = 0; i < iterations; i++) {
            executor.execute();
        }
    }

    private static long[] runMeasurement(int iterations, QueryExecutor executor) {
        long[] latencies = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            executor.execute();
            latencies[i] = System.nanoTime() - start;
        }
        return latencies;
    }

    private static BenchmarkResult buildResult(String queryName, long[] latencies) {
        java.util.Arrays.sort(latencies);
        double avgMs = computeAverage(latencies) / 1_000_000.0;
        double p99Ms = computeP99(latencies) / 1_000_000.0;
        long throughput = computeThroughput(latencies);
        return new BenchmarkResult(queryName, avgMs, p99Ms, throughput);
    }

    private static double computeAverage(long[] latencies) {
        long sum = 0;
        for (long l : latencies) {
            sum += l;
        }
        return (double) sum / latencies.length;
    }

    private static double computeP99(long[] latencies) {
        int index = (int) Math.ceil(latencies.length * 0.99) - 1;
        return latencies[Math.max(0, index)];
    }

    private static long computeThroughput(long[] latencies) {
        double avgNanos = computeAverage(latencies);
        if (avgNanos <= 0) {
            return 0;
        }
        return (long) (1_000_000_000.0 / avgNanos);
    }
}
