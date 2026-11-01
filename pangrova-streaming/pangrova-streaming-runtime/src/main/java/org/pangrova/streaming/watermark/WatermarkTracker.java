package org.pangrova.streaming.watermark;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.streaming.store.WatermarkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-table watermark tracker with state machine (architecture 10 §5 DEF-143).
 *
 * <p>Concurrency strategy: per-table stripe locks via {@code tableLocks} guard
 * compound read-then-write operations across the three ConcurrentHashMap
 * instances ({@code tableWatermarks}, {@code tableStates}, {@code lastEventTimes}).
 * Single-read operations (getWatermark, getState) are lock-free.</p>
 */
@ThreadSafe
public class WatermarkTracker {

    private static final Logger LOG = LoggerFactory.getLogger(WatermarkTracker.class);

    private final ConcurrentHashMap<String, Instant> tableWatermarks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WatermarkState> tableStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Instant> lastEventTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> tableLocks = new ConcurrentHashMap<>();
    private final WatermarkStore store;

    public WatermarkTracker(WatermarkStore store) {
        this.store = store;
    }

    /**
     * Restores the persisted watermark for a table before the pipeline starts.
     * A missing value keeps the table uninitialized; load failures propagate so a
     * restart cannot silently treat late data as on-time.
     */
    public void restore(String tableName) {
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());
        synchronized (lock) {
            store.load(tableName).ifPresent(watermark -> {
                tableWatermarks.putIfAbsent(tableName, watermark);
                lastEventTimes.put(tableName, Instant.now());
                if (getState(tableName) == WatermarkState.INITIALIZING) {
                    tableStates.put(tableName, WatermarkState.ACTIVE);
                }
            });
        }
    }

    /** Applies the configured watermark delay to the batch's maximum event time. */
    public void updateWatermarkFromEventTime(String tableName, Instant maxEventTime,
                                             WatermarkConfig config) {
        Instant candidate = maxEventTime.minus(config.watermarkDelay());
        Instant current = getWatermark(tableName);
        if (candidate.isAfter(current)) {
            updateWatermark(tableName, candidate);
        }
    }

    public void updateWatermark(String tableName, Instant watermark) {
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());
        synchronized (lock) {
            WatermarkState currentState = getState(tableName);
            if (currentState == WatermarkState.FAILED) return;
            Instant current = tableWatermarks.get(tableName);
            if (current != null && !watermark.isAfter(current)) {
                // Late or out-of-order events are normal; never poison the state machine.
                LOG.debug("event=watermark_non_advancing table={} current={} candidate={}",
                        tableName, current, watermark);
                return;
            }
            advanceWatermark(tableName, watermark);
        }
    }

    public Instant inferWatermark(String tableName, List<Instant> eventTimes,
                                  WatermarkConfig config) {
        if (eventTimes.isEmpty()) return getWatermark(tableName);
        Instant maxEventTime = eventTimes.stream().max(Instant::compareTo).orElseThrow();
        return maxEventTime.minus(config.watermarkDelay());
    }

    public Instant getWatermark(String tableName) {
        return tableWatermarks.getOrDefault(tableName, Instant.MIN);
    }

    public WatermarkState getState(String tableName) {
        return tableStates.getOrDefault(tableName, WatermarkState.INITIALIZING);
    }

    public void checkIdle(String tableName, Duration idleTimeout) {
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());
        synchronized (lock) {
            if (getState(tableName) != WatermarkState.ACTIVE) return;
            Instant lastEvent = lastEventTimes.get(tableName);
            if (lastEvent != null && Instant.now().isAfter(lastEvent.plus(idleTimeout))) {
                tableStates.put(tableName, WatermarkState.IDLE);
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
        Instant watermark = tableWatermarks.get(tableName);
        if (watermark == null || watermark.equals(Instant.MIN)) {
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
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());
        synchronized (lock) {
            tableStates.put(tableName, WatermarkState.FAILED);
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
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());
        synchronized (lock) {
            boolean reset = tableStates.replace(tableName, WatermarkState.FAILED, WatermarkState.INITIALIZING);
            if (reset) {
                LOG.info("event=watermark_reset_failed table={} newState=INITIALIZING", tableName);
            }
            return reset;
        }
    }

    /**
     * Removes all tracking state for a table.
     *
     * <p>BUG-09 fix: Uses {@code compute()} on tableLocks to atomically
     * check-and-remove the lock entry. While holding the lock inside the
     * compute lambda, we clean up all three state maps. A concurrent
     * {@code updateWatermark()} calling {@code computeIfAbsent()} on the
     * same key will block on the ConcurrentHashMap segment lock until
     * our compute completes and returns null (removing the entry).
     * This eliminates the window where updateWatermark creates a new
     * lock object after removeTable has already removed the old one.</p>
     *
     * @param tableName the table to remove
     */
    public void removeTable(String tableName) {
        tableLocks.compute(tableName, (key, lock) -> {
            if (lock != null) {
                synchronized (lock) {
                    tableWatermarks.remove(tableName);
                    tableStates.remove(tableName);
                    lastEventTimes.remove(tableName);
                }
            } else {
                tableWatermarks.remove(tableName);
                tableStates.remove(tableName);
                lastEventTimes.remove(tableName);
            }
            return null; // remove the entry from tableLocks
        });
        LOG.debug("event=watermark_table_removed table={}", tableName);
    }

    private void advanceWatermark(String tableName, Instant watermark) {
        // Persist first, then update in-memory. If store.save() fails,
        // in-memory state stays at the old watermark — no data loss on restart.
        store.save(tableName, watermark);
        tableWatermarks.put(tableName, watermark);
        lastEventTimes.put(tableName, Instant.now());
        // BUG-004 fix: Re-read state before transitioning. A concurrent
        // markFailed() call (which holds the same per-table lock) may have
        // set FAILED between store.save() and here. Guard against overwriting
        // FAILED with ACTIVE.
        transitionToActive(tableName);
    }

    private void transitionToActive(String tableName) {
        WatermarkState freshState = tableStates.getOrDefault(tableName, WatermarkState.INITIALIZING);
        if (freshState == WatermarkState.FAILED) {
            LOG.debug("event=watermark_transition_skipped table={} reason=state_is_FAILED", tableName);
            return;
        }
        if (freshState != WatermarkState.ACTIVE) {
            tableStates.put(tableName, WatermarkState.ACTIVE);
        }
    }
}
