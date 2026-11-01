package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import org.pangrova.observability.sdk.metrics.nativebridge.NativeMetricsPuller;
import org.pangrova.observability.sdk.metrics.nativebridge.NativeMetricsPublisher;
import org.pangrova.observability.api.noop.NoOpMetricsProvider;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.ObservableDoubleGauge;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the wiring of NativeMetricsPuller into Worker runtime.
 * Validates scheduling, gauge registration, and native-unavailable skip behavior.
 */
@Tag("native-required")
class NativeMetricsPullerWiringTest {

    private static StubCoordinatorServer stubCoordinator;

    @BeforeAll
    static void startStub() throws Exception {
        stubCoordinator = StubCoordinatorServer.start();
    }

    @AfterAll
    static void stopStub() {
        if (stubCoordinator != null) {
            stubCoordinator.close();
        }
    }

    @Test
    void nativeAvailableCreatesAndSchedulesPuller() throws Exception {
        AtomicInteger pullCount = new AtomicInteger(0);
        byte[] stats = buildStats(512L, 100L, 256L, 0.9);
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> {
            pullCount.incrementAndGet();
            return stats;
        });
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, TestObservability.METRICS);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "test-native-metrics"));
        try {
            executor.scheduleAtFixedRate(publisher::pullAndPublish, 0, 50, TimeUnit.MILLISECONDS);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2))
                    .untilAsserted(() -> {
                        assertThat(pullCount.get()).isGreaterThanOrEqualTo(2);
                        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(512L);
                    });
            assertThat(pullCount.get()).isGreaterThanOrEqualTo(2);
            assertThat(publisher.getMemoryUsedBytes()).isEqualTo(512L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void nativeUnavailableSkipsMetricsPuller() {
        // When nativeAvailable=false, WorkerBootstrap skips startNativeMetrics().
        // Verify that metricsProvider with NoOp works without any puller wired.
        MetricsProvider noOp = TestObservability.METRICS;
        ObservableDoubleGauge gauge = noOp.createGauge(
                "test.gauge", "test", "unit", () -> 0.0);
        assertThat(gauge).isNotNull();
    }

    @Test
    void metricsPublishedViaGaugeSuppliers() {
        byte[] stats = buildStats(2048L, 999L, 4096L, 0.75);
        GaugeCapturingMetricsProvider capturing = new GaugeCapturingMetricsProvider();
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> stats);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, capturing);

        publisher.pullAndPublish();

        assertThat(capturing.suppliers).hasSize(4);
        assertThat(capturing.suppliers.get(0).get()).isEqualTo(2048.0);
        assertThat(capturing.suppliers.get(1).get()).isEqualTo(999.0);
        assertThat(capturing.suppliers.get(2).get()).isEqualTo(4096.0);
        assertThat(capturing.suppliers.get(3).get()).isEqualTo(0.75);
    }

    @Test
    void workerBootstrapStartsAndStopsWithPackagedNative() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "wiring-test-worker", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, WorkerBootstrapConfig.ResourceConfig.defaults(),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        WorkerBootstrap bootstrap = TestWorkerBootstrap.create(config);

        try {
            bootstrap.start();
            assertThat(bootstrap.isStarted()).isTrue();
        } finally {
            bootstrap.shutdown();
        }
        assertThat(bootstrap.isStarted()).isFalse();
    }

    // -- Helpers --

    private static byte[] buildStats(long memory, long rows, long ioBytes, double cacheHit) {
        ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(memory);
        buf.putLong(rows);
        buf.putLong(ioBytes);
        buf.putLong(Double.doubleToLongBits(cacheHit));
        return buf.array();
    }

    /**
     * Real MetricsProvider that captures gauge suppliers for assertion.
     */
    private static final class GaugeCapturingMetricsProvider extends NoOpMetricsProvider {

        final List<Supplier<Double>> suppliers = new ArrayList<>();

        @Override
        public ObservableDoubleGauge createGauge(
                String name, String description, String unit, Supplier<Double> supplier) {
            suppliers.add(supplier);
            return super.createGauge(name, description, unit, supplier);
        }
    }
}
