package org.pangrova.streaming.property.watermark;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.pangrova.streaming.watermark.WatermarkState;
import org.pangrova.streaming.watermark.WatermarkTracker;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link WatermarkTracker} watermark monotonicity
 * and state machine transitions.
 */
class WatermarkTrackerMonotonicityPropertyTest {

    @Property(tries = 10)
    void watermarkNeverRegresses(
            @ForAll @IntRange(min = 1, max = 20) int updateCount) {
        WatermarkTracker tracker = newTracker();
        String table = "orders";
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        Instant maxSeen = Instant.MIN;
        for (int i = 0; i < updateCount; i++) {
            Instant wm = base.plusSeconds(i);
            tracker.updateWatermark(table, wm);
            if (wm.isAfter(maxSeen)) {
                maxSeen = wm;
            }
            assertThat(tracker.getWatermark(table))
                    .as("Watermark must never regress below max seen")
                    .isAfterOrEqualTo(maxSeen);
        }
    }

    @Property(tries = 10)
    void outOfOrderUpdatesPreserveHighWatermark(
            @ForAll List<@IntRange(min = 0, max = 100) Integer> offsets) {
        Assume.that(!offsets.isEmpty());
        WatermarkTracker tracker = newTracker();
        String table = "events";
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        Instant maxApplied = Instant.MIN;
        for (int offset : offsets) {
            Instant wm = base.plusSeconds(offset);
            tracker.updateWatermark(table, wm);
            if (wm.isAfter(maxApplied)) {
                maxApplied = wm;
            }
        }
        assertThat(tracker.getWatermark(table)).isEqualTo(maxApplied);
    }

    @Property(tries = 10)
    void failedStateBlocksAllUpdates(
            @ForAll @IntRange(min = 1, max = 10) int attempts) {
        WatermarkTracker tracker = newTracker();
        String table = "blocked";
        Instant initial = Instant.parse("2026-01-01T00:00:00Z");
        tracker.updateWatermark(table, initial);
        tracker.markFailed(table, new RuntimeException("test"));

        for (int i = 1; i <= attempts; i++) {
            tracker.updateWatermark(table, initial.plusSeconds(i * 100));
        }
        assertThat(tracker.getWatermark(table)).isEqualTo(initial);
        assertThat(tracker.getState(table)).isEqualTo(WatermarkState.FAILED);
    }

    @Property(tries = 10)
    void resetFailedAllowsResumption(
            @ForAll @IntRange(min = 1, max = 10) int postResetUpdates) {
        WatermarkTracker tracker = newTracker();
        String table = "resumable";
        Instant initial = Instant.parse("2026-01-01T00:00:00Z");
        tracker.updateWatermark(table, initial);
        tracker.markFailed(table, new RuntimeException("test"));
        tracker.resetFailed(table);

        Instant latest = initial;
        for (int i = 1; i <= postResetUpdates; i++) {
            latest = initial.plusSeconds(i);
            tracker.updateWatermark(table, latest);
        }
        assertThat(tracker.getWatermark(table)).isEqualTo(latest);
        assertThat(tracker.getState(table)).isEqualTo(WatermarkState.ACTIVE);
    }

    @Property(tries = 10)
    void regressionTransitionsActiveToStale(
            @ForAll @IntRange(min = 2, max = 50) int highSeconds) {
        WatermarkTracker tracker = newTracker();
        String table = "stale_test";
        Instant high = Instant.parse("2026-01-01T00:00:00Z").plusSeconds(highSeconds);
        Instant low = Instant.parse("2026-01-01T00:00:00Z");

        tracker.updateWatermark(table, high);
        assertThat(tracker.getState(table)).isEqualTo(WatermarkState.ACTIVE);

        tracker.updateWatermark(table, low);
        assertThat(tracker.getState(table)).isEqualTo(WatermarkState.STALE);
        assertThat(tracker.getWatermark(table)).isEqualTo(high);
    }

    private static WatermarkTracker newTracker() {
        return new WatermarkTracker(new InMemoryStore());
    }

    static class InMemoryStore implements WatermarkStore {
        private final ConcurrentHashMap<String, Instant> data = new ConcurrentHashMap<>();
        @Override public void save(String t, Instant w) { data.put(t, w); }
        @Override public Optional<Instant> load(String t) { return Optional.ofNullable(data.get(t)); }
    }
}
