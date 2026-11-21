package org.pangrova.streaming.watermark;

import org.pangrova.streaming.enums.WatermarkState;

import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WatermarkTracker} covering BUG-ST01: FAILED dead end fix.
 */
class WatermarkTrackerTest {

    private InMemoryWatermarkStore store;
    private WatermarkTracker tracker;

    @BeforeEach
    void setUp() {
        store = new InMemoryWatermarkStore();
        tracker = new WatermarkTracker(store);
    }

    @Test
    @DisplayName("resetFailed transitions FAILED -> INITIALIZING")
    void resetFailedTransitionsToInitializing() {
        tracker.markFailed("orders", new RuntimeException("connection lost"));
        assertThat(tracker.getState("orders")).isEqualTo(WatermarkState.FAILED);

        boolean reset = tracker.resetFailed("orders");

        assertThat(reset).isTrue();
        assertThat(tracker.getState("orders")).isEqualTo(WatermarkState.INITIALIZING);
    }

    @Test
    @DisplayName("resetFailed returns false for non-FAILED state")
    void resetFailedReturnsFalseForNonFailedState() {
        // INITIALIZING (default) -> resetFailed should be no-op
        assertThat(tracker.resetFailed("orders")).isFalse();

        // Drive to ACTIVE
        tracker.updateWatermark("orders", Instant.now());
        assertThat(tracker.getState("orders")).isEqualTo(WatermarkState.ACTIVE);
        assertThat(tracker.resetFailed("orders")).isFalse();
        assertThat(tracker.getState("orders")).isEqualTo(WatermarkState.ACTIVE);
    }

    @Test
    @DisplayName("After resetFailed, updateWatermark resumes normally")
    void afterResetUpdateWatermarkResumes() {
        tracker.markFailed("orders", new RuntimeException("timeout"));
        tracker.resetFailed("orders");

        Instant watermark = Instant.parse("2026-03-16T10:00:00Z");
        tracker.updateWatermark("orders", watermark);

        assertThat(tracker.getState("orders")).isEqualTo(WatermarkState.ACTIVE);
        assertThat(tracker.getWatermark("orders")).isEqualTo(watermark);
    }

    @Test
    @DisplayName("FAILED state blocks updateWatermark")
    void failedStateBlocksUpdate() {
        Instant initial = Instant.parse("2026-03-16T09:00:00Z");
        tracker.updateWatermark("orders", initial);
        tracker.markFailed("orders", new RuntimeException("crash"));

        Instant later = Instant.parse("2026-03-16T10:00:00Z");
        tracker.updateWatermark("orders", later);

        // Watermark should not advance while FAILED
        assertThat(tracker.getWatermark("orders")).isEqualTo(initial);
        assertThat(tracker.getState("orders")).isEqualTo(WatermarkState.FAILED);
    }

    @Test
    @DisplayName("resetFailed on unknown table returns false")
    void resetFailedOnUnknownTableReturnsFalse() {
        assertThat(tracker.resetFailed("nonexistent")).isFalse();
    }

    @Test
    void staleTrackerUsesThePositionReturnedByTheSharedAuthority() {
        Instant newest = Instant.parse("2026-03-16T10:00:00Z");
        tracker.updateWatermark("orders", newest);
        var restarted = new WatermarkTracker(store);
        restarted.updateWatermark("orders", newest.minusSeconds(1));
        assertThat(restarted.getWatermark("orders")).isEqualTo(newest);
        assertThat(store.load("orders")).contains(newest);
    }

    @Test
    void restartSeesTheCommittedPositionBeforePollingTheFirstBatch() {
        Instant committed = Instant.parse("2026-03-16T10:00:00Z");
        store.advance("orders", committed);
        tracker.restore("orders");
        assertThat(tracker.getWatermark("orders")).isEqualTo(committed);
        assertThat(tracker.getState("orders")).isEqualTo(WatermarkState.INITIALIZING);
    }

    @Test
    void releasingAnObservationDuringAStoreWriteCannotReplaceTheNewLocalState() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var resume = new java.util.concurrent.CountDownLatch(1);
        var committed = Instant.parse("2026-03-16T10:00:00Z");
        var delayed = new InMemoryWatermarkStore() {
            @Override
            public Instant advance(String table, Instant position) {
                entered.countDown();
                try {
                    if (!resume.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("timeout");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return super.advance(table, position);
            }
        };
        var local = new WatermarkTracker(delayed);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var write = executor.submit(() -> local.updateWatermark("orders", committed));
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            local.removeTable("orders");
            local.markFailed("orders", new IllegalStateException("new owner"));
            resume.countDown();
            write.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(delayed.load("orders")).contains(committed);
            assertThat(local.getState("orders")).isEqualTo(WatermarkState.FAILED);
            assertThat(local.getWatermark("orders")).isEqualTo(Instant.MIN);
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    /**
     * Simple in-memory WatermarkStore for testing without external dependencies.
     */
    static class InMemoryWatermarkStore implements WatermarkStore {
        private final ConcurrentHashMap<String, Instant> data = new ConcurrentHashMap<>();

        @Override
        public Instant advance(String tableName, Instant watermark) {
            return data.merge(tableName, watermark, (a, b) -> a.isAfter(b) ? a : b);
        }

        @Override
        public Optional<Instant> load(String tableName) {
            return Optional.ofNullable(data.get(tableName));
        }
    }
}
