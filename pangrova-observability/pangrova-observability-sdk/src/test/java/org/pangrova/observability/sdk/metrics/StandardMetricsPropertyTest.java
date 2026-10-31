package org.pangrova.observability.sdk.metrics;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.observability.sdk.metrics.jvm.JvmMetricsCollector;
import org.pangrova.observability.api.noop.NoOpMetricsProvider;
import org.pangrova.observability.api.DoubleHistogram;
import org.pangrova.observability.api.LongCounter;
import org.pangrova.observability.api.LongUpDownCounter;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.ObservableDoubleGauge;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for standard metrics collectors.
 *
 * <p>Validates: Requirements E.2
 */
class StandardMetricsPropertyTest {

    /**
     * Property: HTTP error count never exceeds total request count.
     *
     * <p>Validates: Requirements E.2
     */
    @Property(tries = 200)
    void errorCountNeverExceedsTotalRequests(
            @ForAll @IntRange(min = 100, max = 599) int statusCode,
            @ForAll @DoubleRange(min = 0.0, max = 60000.0) double durationMs) {
        var counting = new CountingMetricsProvider();
        var collector = new HttpMetricsCollector(counting);

        collector.recordRequest(statusCode, durationMs);

        assertThat(counting.errorCount.get()).isLessThanOrEqualTo(counting.requestCount.get());
    }

    /**
     * Property: Every request increments the request counter by exactly 1.
     *
     * <p>Validates: Requirements E.2
     */
    @Property(tries = 200)
    void everyRequestIncrementsCounterByOne(
            @ForAll @IntRange(min = 100, max = 599) int statusCode,
            @ForAll @DoubleRange(min = 0.0, max = 60000.0) double durationMs) {
        var counting = new CountingMetricsProvider();
        var collector = new HttpMetricsCollector(counting);

        long before = counting.requestCount.get();
        collector.recordRequest(statusCode, durationMs);
        long after = counting.requestCount.get();

        assertThat(after - before).isEqualTo(1L);
    }

    /**
     * Property: Status codes below 400 never increment error counter.
     *
     * <p>Validates: Requirements E.2
     */
    @Property(tries = 200)
    void statusBelow400NeverCountsAsError(
            @ForAll @IntRange(min = 100, max = 399) int statusCode,
            @ForAll @DoubleRange(min = 0.0, max = 60000.0) double durationMs) {
        var counting = new CountingMetricsProvider();
        var collector = new HttpMetricsCollector(counting);

        collector.recordRequest(statusCode, durationMs);

        assertThat(counting.errorCount.get()).isEqualTo(0L);
    }

    /**
     * Property: Status codes 400 and above always increment error counter.
     *
     * <p>Validates: Requirements E.2
     */
    @Property(tries = 200)
    void statusAtOrAbove400AlwaysCountsAsError(
            @ForAll @IntRange(min = 400, max = 599) int statusCode,
            @ForAll @DoubleRange(min = 0.0, max = 60000.0) double durationMs) {
        var counting = new CountingMetricsProvider();
        var collector = new HttpMetricsCollector(counting);

        collector.recordRequest(statusCode, durationMs);

        assertThat(counting.errorCount.get()).isEqualTo(1L);
    }

    /**
     * Property: JVM GC count aggregation is always non-negative regardless of
     * individual GC bean values (handles -1 for undefined).
     *
     * <p>Validates: Requirements E.2
     */
    @Property(tries = 200)
    void gcCountAggregationAlwaysNonNegative(
            @ForAll @LongRange(min = -1, max = 1_000_000) long gc1Count,
            @ForAll @LongRange(min = -1, max = 1_000_000) long gc2Count) {
        var gc1 = new StubGarbageCollectorMXBean(gc1Count, 0);
        var gc2 = new StubGarbageCollectorMXBean(gc2Count, 0);

        var collector = new JvmMetricsCollector(
                new NoOpMetricsProvider(),
                ManagementFactory.getMemoryMXBean(),
                ManagementFactory.getThreadMXBean(),
                List.of(gc1, gc2));

        assertThat(collector.getTotalGcCount()).isGreaterThanOrEqualTo(0.0);
    }

    /**
     * Property: JVM GC time aggregation is always non-negative regardless of
     * individual GC bean values (handles -1 for undefined).
     *
     * <p>Validates: Requirements E.2
     */
    @Property(tries = 200)
    void gcTimeAggregationAlwaysNonNegative(
            @ForAll @LongRange(min = -1, max = 1_000_000) long gc1Time,
            @ForAll @LongRange(min = -1, max = 1_000_000) long gc2Time) {
        var gc1 = new StubGarbageCollectorMXBean(0, gc1Time);
        var gc2 = new StubGarbageCollectorMXBean(0, gc2Time);

        var collector = new JvmMetricsCollector(
                new NoOpMetricsProvider(),
                ManagementFactory.getMemoryMXBean(),
                ManagementFactory.getThreadMXBean(),
                List.of(gc1, gc2));

        assertThat(collector.getTotalGcTime()).isGreaterThanOrEqualTo(0.0);
    }

    /**
     * Property: JVM GC aggregation correctly sums only non-negative values.
     *
     * <p>Validates: Requirements E.2
     */
    @Property(tries = 200)
    void gcAggregationSumsOnlyNonNegativeValues(
            @ForAll @LongRange(min = 0, max = 1_000_000) long gc1Count,
            @ForAll @LongRange(min = 0, max = 1_000_000) long gc2Count) {
        var gc1 = new StubGarbageCollectorMXBean(gc1Count, 0);
        var gc2 = new StubGarbageCollectorMXBean(gc2Count, 0);

        var collector = new JvmMetricsCollector(
                new NoOpMetricsProvider(),
                ManagementFactory.getMemoryMXBean(),
                ManagementFactory.getThreadMXBean(),
                List.of(gc1, gc2));

        assertThat(collector.getTotalGcCount()).isEqualTo((double) (gc1Count + gc2Count));
    }

    /**
     * Counting provider that tracks request and error counts via atomic counters.
     */
    private static final class CountingMetricsProvider implements MetricsProvider {
        final AtomicLong requestCount = new AtomicLong();
        final AtomicLong errorCount = new AtomicLong();
        private boolean firstCounter = true;

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            if (firstCounter) {
                firstCounter = false;
                return requestCount::addAndGet;
            }
            return errorCount::addAndGet;
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            return new ObservableDoubleGauge() {};
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                                String unit, List<Double> boundaries) {
            return value -> {};
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return value -> {};
        }
    }

    /**
     * Stub GC MXBean for property testing with configurable values.
     */
    private static final class StubGarbageCollectorMXBean implements GarbageCollectorMXBean {
        private final long collectionCount;
        private final long collectionTime;

        StubGarbageCollectorMXBean(long collectionCount, long collectionTime) {
            this.collectionCount = collectionCount;
            this.collectionTime = collectionTime;
        }

        @Override public long getCollectionCount() { return collectionCount; }
        @Override public long getCollectionTime() { return collectionTime; }
        @Override public String[] getMemoryPoolNames() { return new String[0]; }
        @Override public String getName() { return "StubGC"; }
        @Override public boolean isValid() { return true; }
        @Override public javax.management.ObjectName getObjectName() { return null; }
    }
}
