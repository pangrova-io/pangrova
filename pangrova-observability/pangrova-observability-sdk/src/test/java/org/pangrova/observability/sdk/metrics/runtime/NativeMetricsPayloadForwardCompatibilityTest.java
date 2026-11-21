package org.pangrova.observability.sdk.metrics.runtime;

import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Forward-compatibility tests for versioned native metrics payload.
 *
 * Validates that the current decoder handles:
 * - Legacy (version 0) 32-byte payloads
 * - Version 1 payloads with header
 * - Future version payloads with extra trailing fields (graceful degradation)
 * - Unknown version payloads (graceful degradation)
 * - Truncated payloads (missing fields default to zero)
 *
 * **Validates: Requirements 1.60, 2.60**
 */
class NativeMetricsPayloadForwardCompatibilityTest {

    @Test
    void legacyPayloadDecodedAsVersion0() {
        byte[] legacy = buildLegacyPayload(512L, 1000L, 256L, 0.75);

        NativeMetricsPayload payload = NativeMetricsPayload.decode(legacy);

        assertThat(payload.schemaVersion()).isEqualTo(NativeMetricsPayload.LEGACY_VERSION);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(512L);
        assertThat(payload.getLong("operatorRowsProcessed")).isEqualTo(1000L);
        assertThat(payload.getLong("ioBytesRead")).isEqualTo(256L);
        assertThat(payload.getDouble("cacheHitRatio")).isEqualTo(0.75);
    }

    @Test
    void version1PayloadDecodedCorrectly() {
        byte[] v1 = NativeMetricsPayload.encodeV1(2048L, 9999L, 4096L, 0.92);

        NativeMetricsPayload payload = NativeMetricsPayload.decode(v1);

        assertThat(payload.schemaVersion()).isEqualTo(NativeMetricsPayload.VERSION_1);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(2048L);
        assertThat(payload.getLong("operatorRowsProcessed")).isEqualTo(9999L);
        assertThat(payload.getLong("ioBytesRead")).isEqualTo(4096L);
        assertThat(payload.getDouble("cacheHitRatio")).isEqualTo(0.92);
    }

    @Test
    void futureVersion2WithExtraFieldsDecodesKnownFieldsOnly() {
        // Simulate a "version 2" payload: header(4) + 4 known fields(32) + 2 extra fields(16)
        ByteBuffer buf = ByteBuffer.allocate(4 + 32 + 16).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(2);                                      // schemaVersion = 2
        buf.putLong(8192L);                                 // memoryUsedBytes
        buf.putLong(50000L);                                // operatorRowsProcessed
        buf.putLong(16384L);                                // ioBytesRead
        buf.putLong(Double.doubleToLongBits(0.99));         // cacheHitRatio
        buf.putLong(777L);                                  // hypothetical new field 1
        buf.putLong(888L);                                  // hypothetical new field 2

        NativeMetricsPayload payload = NativeMetricsPayload.decode(buf.array());

        // Known fields decoded correctly
        assertThat(payload.schemaVersion()).isEqualTo(2);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(8192L);
        assertThat(payload.getLong("operatorRowsProcessed")).isEqualTo(50000L);
        assertThat(payload.getLong("ioBytesRead")).isEqualTo(16384L);
        assertThat(payload.getDouble("cacheHitRatio")).isEqualTo(0.99);
        // Extra fields are silently ignored — no crash, no error
    }

    @Test
    void unknownVersion99GracefullyDegradesWithKnownFields() {
        // Version 99 with same field layout as V1
        ByteBuffer buf = ByteBuffer.allocate(4 + 32).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(99);                                     // unknown schemaVersion
        buf.putLong(1111L);                                 // memoryUsedBytes
        buf.putLong(2222L);                                 // operatorRowsProcessed
        buf.putLong(3333L);                                 // ioBytesRead
        buf.putLong(Double.doubleToLongBits(0.55));         // cacheHitRatio

        NativeMetricsPayload payload = NativeMetricsPayload.decode(buf.array());

        assertThat(payload.schemaVersion()).isEqualTo(99);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(1111L);
        assertThat(payload.getLong("operatorRowsProcessed")).isEqualTo(2222L);
        assertThat(payload.getLong("ioBytesRead")).isEqualTo(3333L);
        assertThat(payload.getDouble("cacheHitRatio")).isEqualTo(0.55);
    }

    @Test
    void truncatedVersionedPayloadDefaultsMissingFieldsToZero() {
        // Version 1 header + only 2 fields (missing ioBytesRead and cacheHitRatio)
        ByteBuffer buf = ByteBuffer.allocate(4 + 16).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(1);                                      // schemaVersion = 1
        buf.putLong(4096L);                                 // memoryUsedBytes
        buf.putLong(7777L);                                 // operatorRowsProcessed
        // ioBytesRead and cacheHitRatio missing

        NativeMetricsPayload payload = NativeMetricsPayload.decode(buf.array());

        assertThat(payload.schemaVersion()).isEqualTo(1);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(4096L);
        assertThat(payload.getLong("operatorRowsProcessed")).isEqualTo(7777L);
        assertThat(payload.getLong("ioBytesRead")).isEqualTo(0L);
        assertThat(payload.getDouble("cacheHitRatio")).isEqualTo(0.0);
        assertThat(payload.fields().get("ioBytesRead")).isInstanceOf(Long.class);
        assertThat(payload.fields().get("cacheHitRatio")).isInstanceOf(Double.class);
    }

    @Test
    void nullPayloadReturnsAllZeros() {
        NativeMetricsPayload payload = NativeMetricsPayload.decode(null);

        assertThat(payload.schemaVersion()).isEqualTo(NativeMetricsPayload.LEGACY_VERSION);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(0L);
        assertThat(payload.getLong("operatorRowsProcessed")).isEqualTo(0L);
        assertThat(payload.getLong("ioBytesRead")).isEqualTo(0L);
        assertThat(payload.getDouble("cacheHitRatio")).isEqualTo(0.0);
    }

    @Test
    void emptyPayloadReturnsAllZeros() {
        NativeMetricsPayload payload = NativeMetricsPayload.decode(new byte[0]);

        assertThat(payload.schemaVersion()).isEqualTo(NativeMetricsPayload.LEGACY_VERSION);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(0L);
        assertThat(payload.fields().get("memoryUsedBytes")).isInstanceOf(Long.class);
        assertThat(payload.fields().get("operatorRowsProcessed")).isInstanceOf(Long.class);
        assertThat(payload.fields().get("ioBytesRead")).isInstanceOf(Long.class);
        assertThat(payload.fields().get("cacheHitRatio")).isInstanceOf(Double.class);
    }

    @Test
    void tooShortForHeaderReturnsAllZeros() {
        // 3 bytes — not enough for a version header
        NativeMetricsPayload payload = NativeMetricsPayload.decode(new byte[3]);

        assertThat(payload.schemaVersion()).isEqualTo(NativeMetricsPayload.LEGACY_VERSION);
        assertThat(payload.getLong("memoryUsedBytes")).isEqualTo(0L);
    }

    @Test
    void futureVersionDecodedByPublisherGracefullyDegrades() {
        // Simulate version 2 payload with extra fields, fed through the full publisher pipeline
        ByteBuffer buf = ByteBuffer.allocate(4 + 32 + 8).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(2);
        buf.putLong(6000L);
        buf.putLong(12000L);
        buf.putLong(3000L);
        buf.putLong(Double.doubleToLongBits(0.88));
        buf.putLong(999L);  // extra field

        byte[] v2Payload = buf.array();
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> v2Payload);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, new NoOpMetricsProvider());

        publisher.pullAndPublish();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(6000L);
        assertThat(publisher.getOperatorRowsProcessed()).isEqualTo(12000L);
        assertThat(publisher.getIoBytesRead()).isEqualTo(3000L);
        assertThat(publisher.getCacheHitRatio()).isEqualTo(0.88);
    }

    @Test
    void encodeV1ThenDecodeIsIdempotent() {
        long mem = 123456L;
        long rows = 789012L;
        long io = 345678L;
        double ratio = 0.67;

        byte[] encoded = NativeMetricsPayload.encodeV1(mem, rows, io, ratio);
        NativeMetricsPayload decoded = NativeMetricsPayload.decode(encoded);

        assertThat(decoded.schemaVersion()).isEqualTo(NativeMetricsPayload.VERSION_1);
        assertThat(decoded.getLong("memoryUsedBytes")).isEqualTo(mem);
        assertThat(decoded.getLong("operatorRowsProcessed")).isEqualTo(rows);
        assertThat(decoded.getLong("ioBytesRead")).isEqualTo(io);
        assertThat(decoded.getDouble("cacheHitRatio")).isEqualTo(ratio);
    }

    @Test
    void unknownFieldNameReturnsZero() {
        byte[] v1 = NativeMetricsPayload.encodeV1(100L, 200L, 300L, 0.5);
        NativeMetricsPayload payload = NativeMetricsPayload.decode(v1);

        assertThat(payload.getLong("nonExistentField")).isEqualTo(0L);
        assertThat(payload.getDouble("nonExistentField")).isEqualTo(0.0);
    }

    // -- Helpers --

    private static byte[] buildLegacyPayload(long memory, long rows, long ioBytes, double cacheHit) {
        ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(memory);
        buf.putLong(rows);
        buf.putLong(ioBytes);
        buf.putLong(Double.doubleToLongBits(cacheHit));
        return buf.array();
    }
}
