package org.pangrova.observability.sdk.metrics.jvm;

import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.junit.jupiter.api.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link JvmMetricsCollector}.
 *
 * <p>Verifies that JVM metrics are correctly read from MXBeans and
 * registered as gauges via the Metrics SPI.
 */
class JvmMetricsCollectorTest {

    @Test
    void platformBeansAreRetainedAsLiveManagementHandles() throws Exception {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider(), memory, threads,
                ManagementFactory.getGarbageCollectorMXBeans());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Thread observed = new Thread(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }, "jvm-metrics-live-handle-test");
        try {
            observed.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(threads.getThreadInfo(observed.getId())).isNotNull();
            long publicPeak = threads.getPeakThreadCount();
            assertThat(collector.getPeakThreadCount()).isGreaterThanOrEqualTo((double) publicPeak);
            long maximum = memory.getHeapMemoryUsage().getMax();
            assertThat(collector.getHeapMax()).isEqualTo(maximum < 0 ? 0.0 : (double) maximum);
        } finally {
            release.countDown();
            observed.join(5_000);
            assertThat(observed.isAlive()).isFalse();
        }
    }

    @Test
    void heapUsedReturnsPositiveValue() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getHeapUsed()).isPositive();
    }

    @Test
    void heapCommittedReturnsPositiveValue() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getHeapCommitted()).isPositive();
    }

    @Test
    void heapMaxReturnsNonNegativeValue() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getHeapMax()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void heapUsedDoesNotExceedCommitted() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getHeapUsed()).isLessThanOrEqualTo(collector.getHeapCommitted());
    }

    @Test
    void gcCountReturnsNonNegativeValue() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getTotalGcCount()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void gcTimeReturnsNonNegativeValue() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getTotalGcTime()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void threadCountReturnsPositiveValue() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getLiveThreadCount()).isPositive();
    }

    @Test
    void daemonThreadCountReturnsNonNegativeValue() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getDaemonThreadCount()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void peakThreadCountIsAtLeastLiveCount() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        assertThat(collector.getPeakThreadCount()).isGreaterThanOrEqualTo(collector.getLiveThreadCount());
    }

    @Test
    void registersAllExpectedGauges() {
        var capturing = new CapturingMetricsProvider();
        new JvmMetricsCollector(capturing);

        assertThat(capturing.registeredGaugeNames).containsExactlyInAnyOrder(
                "jvm.memory.heap.used",
                "jvm.memory.heap.committed",
                "jvm.memory.heap.max",
                "jvm.gc.count",
                "jvm.gc.time",
                "jvm.threads.live",
                "jvm.threads.daemon",
                "jvm.threads.peak");
    }

    @Test
    void gcMetricsUpdateAfterGarbageCollection() {
        var collector = new JvmMetricsCollector(new NoOpMetricsProvider());
        double gcCountBefore = collector.getTotalGcCount();

        // Allocate and discard objects to trigger GC
        for (int i = 0; i < 100; i++) {
            byte[] garbage = new byte[1024 * 1024];
        }
        System.gc();

        // GC count should be at least what it was before (monotonically non-decreasing)
        double gcCountAfter = collector.getTotalGcCount();
        assertThat(gcCountAfter).isGreaterThanOrEqualTo(gcCountBefore);
    }

    @Test
    void handlesGcBeanReturningNegativeCount() {
        var memoryBean = ManagementFactory.getMemoryMXBean();
        var threadBean = ManagementFactory.getThreadMXBean();
        var negativeGcBean = new StubGarbageCollectorMXBean(-1, -1);

        var collector = new JvmMetricsCollector(
                new NoOpMetricsProvider(), memoryBean, threadBean, List.of(negativeGcBean));

        assertThat(collector.getTotalGcCount()).isEqualTo(0.0);
        assertThat(collector.getTotalGcTime()).isEqualTo(0.0);
    }

    @Test
    void aggregatesMultipleGcBeans() {
        var memoryBean = ManagementFactory.getMemoryMXBean();
        var threadBean = ManagementFactory.getThreadMXBean();
        var gc1 = new StubGarbageCollectorMXBean(5, 100);
        var gc2 = new StubGarbageCollectorMXBean(3, 50);

        var collector = new JvmMetricsCollector(
                new NoOpMetricsProvider(), memoryBean, threadBean, List.of(gc1, gc2));

        assertThat(collector.getTotalGcCount()).isEqualTo(8.0);
        assertThat(collector.getTotalGcTime()).isEqualTo(150.0);
    }

    /**
     * Capturing provider that records gauge names for verification.
     */
    private static final class CapturingMetricsProvider extends NoOpMetricsProvider {
        final List<String> registeredGaugeNames = new ArrayList<>();

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            registeredGaugeNames.add(name);
            return super.createGauge(name, description, unit, supplier);
        }
    }

    /**
     * Stub GC MXBean for testing aggregation and negative value handling.
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
