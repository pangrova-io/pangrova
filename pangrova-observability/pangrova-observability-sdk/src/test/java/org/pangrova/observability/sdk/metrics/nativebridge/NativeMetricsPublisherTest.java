package org.pangrova.observability.sdk.metrics.nativebridge;

import org.pangrova.observability.sdk.noop.NoOpMetricsProvider;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests NativeMetricsPublisher gauge registration and stats parsing.
 * Uses a real capturing MetricsProvider (no mocks).
 */
class NativeMetricsPublisherTest {

    @Test
    void pullAndPublishUpdatesGaugeValuesFromLegacyStats() {
        byte[] stats = buildLegacyStats(1024L, 5000L, 2048L, 0.85);
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> stats);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());

        publisher.pullAndPublish();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(1024L);
        assertThat(publisher.getOperatorRowsProcessed()).isEqualTo(5000L);
        assertThat(publisher.getIoBytesRead()).isEqualTo(2048L);
        assertThat(publisher.getCacheHitRatio()).isEqualTo(0.85);
    }

    @Test
    void pullAndPublishUpdatesGaugeValuesFromVersionedStats() {
        byte[] stats = NativeMetricsPayload.encodeV1(2048L, 10000L, 4096L, 0.95);
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> stats);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());

        publisher.pullAndPublish();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(2048L);
        assertThat(publisher.getOperatorRowsProcessed()).isEqualTo(10000L);
        assertThat(publisher.getIoBytesRead()).isEqualTo(4096L);
        assertThat(publisher.getCacheHitRatio()).isEqualTo(0.95);
    }

    @Test
    void pullAndPublishWithNullStatsResetsToZero() {
        byte[] initialStats = buildLegacyStats(100L, 200L, 300L, 0.5);
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> initialStats);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());
        publisher.pullAndPublish();

        // Now return null stats
        NativeMetricsPuller nullPuller = new NativeMetricsPuller(timeoutMs -> null);
        NativeMetricsPublisher nullPublisher = new NativeMetricsPublisher(nullPuller, new NoOpMetricsProvider());
        nullPublisher.pullAndPublish();

        assertThat(nullPublisher.getMemoryUsedBytes()).isEqualTo(0L);
        assertThat(nullPublisher.getOperatorRowsProcessed()).isEqualTo(0L);
        assertThat(nullPublisher.getIoBytesRead()).isEqualTo(0L);
        assertThat(nullPublisher.getCacheHitRatio()).isEqualTo(0.0);
    }

    @Test
    void pullAndPublishWithShortStatsResetsToZero() {
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> new byte[3]);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());

        publisher.pullAndPublish();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(0L);
        assertThat(publisher.getCacheHitRatio()).isEqualTo(0.0);
    }

    @Test
    void gaugesRegisteredWithCorrectNames() {
        CapturingMetricsProvider capturing = new CapturingMetricsProvider();
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> new byte[0]);
        new NativeMetricsPublisher(puller, capturing);

        assertThat(capturing.registeredGaugeNames).containsExactlyInAnyOrder(
                "pangrova.native.memory_used_bytes",
                "pangrova.native.operator_rows_processed",
                "pangrova.native.io_bytes_read",
                "pangrova.native.cache_hit_ratio");
    }

    @Test
    void pullAndPublishHandlesSupplierException() {
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> {
            throw new RuntimeException("native crash");
        });
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());

        // Should not throw
        publisher.pullAndPublish();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(0L);
    }

    @Test
    void stopResetsGaugesAndPreventsSubsequentPulls() {
        byte[] stats = buildLegacyStats(1024L, 5000L, 2048L, 0.85);
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> stats);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());
        publisher.pullAndPublish();
        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(1024L);

        publisher.stop();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(0L);
        assertThat(publisher.getOperatorRowsProcessed()).isEqualTo(0L);
        assertThat(publisher.getIoBytesRead()).isEqualTo(0L);
        assertThat(publisher.getCacheHitRatio()).isEqualTo(0.0);

        // pullAndPublish after stop is a no-op
        publisher.pullAndPublish();
        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(0L);
    }

    @Test
    void stopIsIdempotent() {
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> new byte[0]);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());

        publisher.stop();
        publisher.stop();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(0L);
    }


    // -- Helpers --

    /**
     * Builds a legacy 32-byte stats payload (version 0, no header).
     */
    private static byte[] buildLegacyStats(long memory, long rows, long ioBytes, double cacheHit) {
        ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(memory);
        buf.putLong(rows);
        buf.putLong(ioBytes);
        buf.putLong(Double.doubleToLongBits(cacheHit));
        return buf.array();
    }

    /**
     * Real MetricsProvider that captures registered gauge names.
     * Delegates to NoOpMetricsProvider for actual gauge instances.
     */
    private static final class CapturingMetricsProvider extends NoOpMetricsProvider {

        final List<String> registeredGaugeNames = new ArrayList<>();

        @Override
        public org.pangrova.spi.observability.ObservableDoubleGauge createGauge(
                String name, String description, String unit, Supplier<Double> supplier) {
            registeredGaugeNames.add(name);
            return super.createGauge(name, description, unit, supplier);
        }
    }
}
