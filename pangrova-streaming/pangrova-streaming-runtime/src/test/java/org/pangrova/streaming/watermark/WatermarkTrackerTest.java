package org.pangrova.streaming.watermark;

import org.pangrova.spi.streaming.store.WatermarkStore;
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
    @DisplayName("restore loads the persisted watermark and activates the table")
    void restoreLoadsPersistedWatermark() {
        Instant persisted = Instant.parse("2026-03-16T08:00:00Z");
        store.save("orders", persisted);

        WatermarkTracker restored = new WatermarkTracker(store);
        restored.restore("orders");

        assertThat(restored.getWatermark("orders")).isEqualTo(persisted);
        assertThat(restored.getState("orders")).isEqualTo(WatermarkState.ACTIVE);
    }

    @Test
    @DisplayName("updateWatermarkFromEventTime applies the configured delay and is monotonic")
    void updateWatermarkFromEventTimeAppliesDelay() {
        WatermarkConfig config = new WatermarkConfig("event_time",
                java.time.Duration.ofSeconds(10), java.time.Duration.ofMinutes(5));
        Instant maxEventTime = Instant.parse("2026-03-16T10:00:30Z");

        tracker.updateWatermarkFromEventTime("orders", maxEventTime, config);
        assertThat(tracker.getWatermark("orders"))
                .isEqualTo(maxEventTime.minus(config.watermarkDelay()));

        tracker.updateWatermarkFromEventTime("orders", maxEventTime.minusSeconds(60), config);
        assertThat(tracker.getWatermark("orders"))
                .isEqualTo(maxEventTime.minus(config.watermarkDelay()));
    }

    /**
     * Simple in-memory WatermarkStore for testing without external dependencies.
     */
    static class InMemoryWatermarkStore implements WatermarkStore {
        private final ConcurrentHashMap<String, Instant> data = new ConcurrentHashMap<>();

        @Override
        public void save(String tableName, Instant watermark) {
            data.put(tableName, watermark);
        }

        @Override
        public Optional<Instant> load(String tableName) {
            return Optional.ofNullable(data.get(tableName));
        }
    }
}
