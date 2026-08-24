package org.pangrova.coordinator.dynamictable.detect;

import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.pangrova.spi.streaming.CdcOperation;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CdcChangeDetector}.
 *
 * <p>Uses hand-written test implementations — no mock frameworks.
 * Covers: empty events, normal CDC batch, schema version mismatch,
 * connection interruption recovery, and CdcLifecycleManager integration.</p>
 *
 * <p><b>Validates: Requirements 4, 14, 20</b></p>
 */
class CdcChangeDetectorTest {

    @Test
    void emptyResult_whenNoEventsPolled() {
        var source = new FixedCdcEventSource(List.of());
        var lifecycle = new RecordingCdcLifecycleManager();
        var detector = new CdcChangeDetector(source, lifecycle);

        ChangeResult result = detector.detect(cdcState("offset-100", 1));

        assertThat(result).isInstanceOf(ChangeResult.Empty.class);
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void cdcBatchResult_whenEventsPolled() {
        var events = List.of(
                insertEnvelope("orders", 1),
                insertEnvelope("orders", 1));
        var source = new FixedCdcEventSource(events);
        var lifecycle = new RecordingCdcLifecycleManager();
        var detector = new CdcChangeDetector(source, lifecycle);

        ChangeResult result = detector.detect(cdcState("offset-100", 1));

        assertThat(result).isInstanceOf(ChangeResult.CdcBatch.class);
        assertThat(result.isEmpty()).isFalse();
        var batch = (ChangeResult.CdcBatch) result;
        assertThat(batch.events()).hasSize(2);
    }

    @Test
    void schemaVersionMismatch_throwsSchemaEvolutionException() {
        var events = List.of(insertEnvelope("orders", 2));
        var source = new FixedCdcEventSource(events);
        var lifecycle = new RecordingCdcLifecycleManager();
        var detector = new CdcChangeDetector(source, lifecycle);

        assertThatThrownBy(() -> detector.detect(cdcState("offset-100", 1)))
                .isInstanceOf(SchemaEvolutionException.class)
                .satisfies(ex -> {
                    var sce = (SchemaEvolutionException) ex;
                    assertThat(sce.expectedVersion()).isEqualTo(1);
                    assertThat(sce.actualVersion()).isEqualTo(2);
                    assertThat(sce.dtName()).isEqualTo("test_dt");
                });
    }

    @Test
    void connectionInterruption_retriesFromCheckpoint() {
        var events = List.of(insertEnvelope("orders", 1));
        var source = new FailThenSucceedEventSource(2, events);
        var lifecycle = new RecordingCdcLifecycleManager();
        var detector = new CdcChangeDetector(source, lifecycle, 3, Duration.ofSeconds(1));

        ChangeResult result = detector.detect(cdcState("offset-100", 1));

        assertThat(result).isInstanceOf(ChangeResult.CdcBatch.class);
        assertThat(source.pollAttempts()).isEqualTo(3); // 2 failures + 1 success
    }

    @Test
    void connectionInterruption_exhaustsRetries_throwsException() {
        var source = new FailThenSucceedEventSource(5, List.of());
        var lifecycle = new RecordingCdcLifecycleManager();
        var detector = new CdcChangeDetector(source, lifecycle, 2, Duration.ofSeconds(1));

        assertThatThrownBy(() -> detector.detect(cdcState("offset-100", 1)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("CDC connection failed after 2 retries");
    }

    @Test
    void lifecycleManager_registersStreamAndRenewsHeartbeat() {
        var events = List.of(insertEnvelope("orders", 1));
        var source = new FixedCdcEventSource(events);
        var lifecycle = new RecordingCdcLifecycleManager();
        var detector = new CdcChangeDetector(source, lifecycle);

        detector.detect(cdcState("offset-100", 1));

        assertThat(lifecycle.registerCount()).isEqualTo(1);
        assertThat(lifecycle.heartbeatCount()).isGreaterThanOrEqualTo(1);
        assertThat(lifecycle.lastRegisteredConsumerId()).isEqualTo("dt-cdc-test_dt");
        assertThat(lifecycle.lastRegisteredTenantId()).isEqualTo("tenant-1");
    }

    @Test
    void heartbeatRenewed_evenWhenDetectThrowsSchemaException() {
        var events = List.of(insertEnvelope("orders", 99));
        var source = new FixedCdcEventSource(events);
        var lifecycle = new RecordingCdcLifecycleManager();
        var detector = new CdcChangeDetector(source, lifecycle);

        assertThatThrownBy(() -> detector.detect(cdcState("offset-100", 1)))
                .isInstanceOf(SchemaEvolutionException.class);

        assertThat(lifecycle.heartbeatCount()).isGreaterThanOrEqualTo(1);
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private static DynamicTableState cdcState(String checkpoint, int schemaVersion) {
        Instant now = Instant.now();
        return new DynamicTableState(
                "test_dt", "tenant-1", DynamicTableLifecycleState.ACTIVE,
                DynamicTableRefreshMode.CDC, Duration.ofMinutes(1),
                checkpoint, "CDC_OFFSET",
                schemaVersion, null, "admin", "coord-1", 1L,
                now, 0L, 0, now, now);
    }

    private static CdcEnvelope insertEnvelope(String tableName, int schemaVersion) {
        return new CdcEnvelope(
                CdcOperation.INSERT, tableName,
                null, null,
                Instant.now(), schemaVersion, Map.of());
    }

    // ── Test implementations ────────────────────────────────────────

    /** Returns a fixed list of events on every poll. */
    static final class FixedCdcEventSource implements CdcEventSource {
        private final List<CdcEnvelope> events;

        FixedCdcEventSource(List<CdcEnvelope> events) {
            this.events = List.copyOf(events);
        }

        @Override
        public List<CdcEnvelope> poll(String offset, Duration timeout) {
            return events;
        }
    }

    /** Fails the first N polls, then succeeds with the given events. */
    static final class FailThenSucceedEventSource implements CdcEventSource {
        private final int failCount;
        private final List<CdcEnvelope> successEvents;
        private final AtomicInteger attempts = new AtomicInteger(0);

        FailThenSucceedEventSource(int failCount, List<CdcEnvelope> successEvents) {
            this.failCount = failCount;
            this.successEvents = List.copyOf(successEvents);
        }

        @Override
        public List<CdcEnvelope> poll(String offset, Duration timeout) {
            int attempt = attempts.incrementAndGet();
            if (attempt <= failCount) {
                throw new RuntimeException("Simulated CDC connection failure attempt=" + attempt);
            }
            return successEvents;
        }

        int pollAttempts() { return attempts.get(); }
    }

    /** Records registration and heartbeat calls for verification. */
    static final class RecordingCdcLifecycleManager implements CdcLifecycleManager {
        private final AtomicInteger registerCalls = new AtomicInteger(0);
        private final AtomicInteger heartbeatCalls = new AtomicInteger(0);
        private final List<String> registeredConsumerIds = new ArrayList<>();
        private final List<String> registeredTenantIds = new ArrayList<>();

        @Override
        public CdcRegistrationResult register(String consumerId, String tenantId,
                                              int maxConcurrentWrites, long maxPendingBytes) {
            registerCalls.incrementAndGet();
            registeredConsumerIds.add(consumerId);
            registeredTenantIds.add(tenantId);
            return new CdcRegistrationResult(
                    "stream-" + consumerId, maxConcurrentWrites,
                    maxPendingBytes, 5000L, 30000L);
        }

        @Override
        public void heartbeat(String streamId) {
            heartbeatCalls.incrementAndGet();
        }

        @Override
        public void expireStaleStreams() { /* no-op */ }

        @Override
        public boolean isAvailable(String tableName) { return true; }

        @Override
        public String resolveOffsetFromSnapshot(String tableName, String snapshotCheckpoint) {
            return "resolved-offset-" + snapshotCheckpoint;
        }

        int registerCount() { return registerCalls.get(); }
        int heartbeatCount() { return heartbeatCalls.get(); }

        String lastRegisteredConsumerId() {
            return registeredConsumerIds.isEmpty() ? null
                    : registeredConsumerIds.get(registeredConsumerIds.size() - 1);
        }

        String lastRegisteredTenantId() {
            return registeredTenantIds.isEmpty() ? null
                    : registeredTenantIds.get(registeredTenantIds.size() - 1);
        }
    }
}
