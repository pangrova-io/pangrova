package org.pangrova.streaming.watermark;

import org.pangrova.streaming.enums.WatermarkState;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks one local observation per table. A removed observation cannot republish itself after I/O. */
@ThreadSafe
public class WatermarkTracker {
    private static final Logger LOG = LoggerFactory.getLogger(WatermarkTracker.class);

    private static final class Observation {
        private volatile Instant watermark = Instant.MIN;
        private volatile WatermarkState state = WatermarkState.INITIALIZING;
        private Instant lastEvent;
    }

    private final ConcurrentHashMap<String, Observation> observations = new ConcurrentHashMap<>();
    private final WatermarkStore store;

    public WatermarkTracker(WatermarkStore store) {
        this.store = java.util.Objects.requireNonNull(store, "store");
    }

    /** Restores a committed position on pipeline startup. Restoration does not count as an incoming event. */
    public void restore(String tableName) {
        var observation = observations.computeIfAbsent(tableName, ignored -> new Observation());
        synchronized (observation) {
            store.load(tableName).filter(position -> position.isAfter(observation.watermark))
                    .ifPresent(position -> observation.watermark = position);
        }
    }

    public void updateWatermark(String tableName, Instant watermark) {
        var observation = observations.computeIfAbsent(tableName, ignored -> new Observation());
        synchronized (observation) {
            if (observation.state == WatermarkState.FAILED) return;
            if (!observation.watermark.equals(Instant.MIN) && !watermark.isAfter(observation.watermark)) {
                if (observation.state == WatermarkState.ACTIVE) observation.state = WatermarkState.STALE;
                return;
            }
            Instant committed = store.advance(tableName, watermark);
            observation.watermark = committed;
            observation.lastEvent = Instant.now();
            if (observation.state != WatermarkState.FAILED) observation.state = WatermarkState.ACTIVE;
        }
    }

    public Instant inferWatermark(String tableName, List<Instant> eventTimes,
                                  WatermarkConfig config) {
        if (eventTimes.isEmpty()) return getWatermark(tableName);
        Instant maxEventTime = eventTimes.stream().max(Instant::compareTo).orElseThrow();
        return maxEventTime.minus(config.watermarkDelay());
    }

    public Instant getWatermark(String tableName) {
        var observation = observations.get(tableName);
        return observation == null ? Instant.MIN : observation.watermark;
    }

    public WatermarkState getState(String tableName) {
        var observation = observations.get(tableName);
        return observation == null ? WatermarkState.INITIALIZING : observation.state;
    }

    public void checkIdle(String tableName, Duration idleTimeout) {
        var observation = observations.get(tableName);
        if (observation == null) return;
        synchronized (observation) {
            if (observation.state == WatermarkState.ACTIVE && observation.lastEvent != null
                    && Instant.now().isAfter(observation.lastEvent.plus(idleTimeout))) {
                observation.state = WatermarkState.IDLE;
            }
        }
    }

    /**
     * Checks if the watermark lag exceeds the threshold and logs an alert.
     *
     * @param tableName     the table to check
     * @param lagThreshold  maximum acceptable lag duration
     * @return true if lag exceeds threshold (alert condition)
     */
    public boolean checkLag(String tableName, Duration lagThreshold) {
        Instant watermark = getWatermark(tableName);
        if (watermark.equals(Instant.MIN)) {
            return false;
        }
        Duration lag = Duration.between(watermark, Instant.now());
        if (lag.compareTo(lagThreshold) > 0) {
            LOG.warn("event=watermark_lag_alert table={} lag={} threshold={}",
                    tableName, lag, lagThreshold);
            return true;
        }
        return false;
    }

    public void markFailed(String tableName, Throwable cause) {
        var observation = observations.computeIfAbsent(tableName, ignored -> new Observation());
        synchronized (observation) {
            observation.state = WatermarkState.FAILED;
        }
        LOG.error("event=watermark_failed table={} error={}", tableName, cause.getMessage());
    }

    /**
     * Resets a FAILED table back to INITIALIZING, allowing watermark tracking to resume.
     * Only transitions from FAILED state; other states are left unchanged.
     *
     * @param tableName the table to reset
     * @return true if the table was in FAILED state and was reset
     */
    public boolean resetFailed(String tableName) {
        var observation = observations.get(tableName);
        if (observation == null) return false;
        synchronized (observation) {
            if (observation.state != WatermarkState.FAILED) return false;
            observation.state = WatermarkState.INITIALIZING;
            return true;
        }
    }

    /** Detaches the local observation. A running store call may only update its detached instance. */
    public void removeTable(String tableName) {
        observations.remove(tableName);
        LOG.debug("event=watermark_table_removed table={}", tableName);
    }
}
