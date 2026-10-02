package org.pangrova.observability.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Pulls native (C++) metrics via a functional interface, avoiding direct NativeRuntime dependency.
 * Implements adaptive exponential backoff on consecutive failures.
 */
public final class NativeMetricsPuller {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMetricsPuller.class);

    private static final long DEFAULT_PULL_INTERVAL_MS = 1000L;
    private static final long DEFAULT_TIMEOUT_MS = 500L;
    private static final long MAX_BACKOFF_MULTIPLIER = 60L;

    private final NativeStatsSupplier statsSupplier;
    private final long pullIntervalMs;
    private final long timeoutMs;
    private final long staleThresholdMs;

    private final AtomicLong lastSuccessTimestamp = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private volatile byte[] lastStats;

    public NativeMetricsPuller(NativeStatsSupplier statsSupplier) {
        this(statsSupplier, DEFAULT_PULL_INTERVAL_MS, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Creates a puller with explicit interval, timeout, and derived staleness threshold.
     */
    public NativeMetricsPuller(NativeStatsSupplier statsSupplier,
                                long pullIntervalMs, long timeoutMs) {
        this.statsSupplier = statsSupplier;
        this.pullIntervalMs = pullIntervalMs;
        this.timeoutMs = timeoutMs;
        this.staleThresholdMs = pullIntervalMs * 3;
    }

    /**
     * Attempts to pull native stats. On failure, applies exponential backoff.
     *
     * @return raw stats bytes, or null on failure
     */
    public byte[] pull() {
        try {
            byte[] stats = statsSupplier.getStats(timeoutMs);
            byte[] snapshot = stats == null ? null : stats.clone();
            lastStats = snapshot;
            lastSuccessTimestamp.set(System.currentTimeMillis());
            consecutiveFailures.set(0);
            return snapshot == null ? null : snapshot.clone();
        } catch (Exception e) {
            long failures = consecutiveFailures.incrementAndGet();
            LOG.warn("Native stats pull failed (consecutive failures: {}): {}",
                    failures, e.getMessage());
            return null;
        }
    }

    /**
     * Returns true if elapsed time since last success exceeds the stale threshold.
     */
    public boolean isStale() {
        long elapsed = System.currentTimeMillis() - lastSuccessTimestamp.get();
        return elapsed > staleThresholdMs;
    }

    /**
     * Computes the next pull delay with exponential backoff, capped at 60x the base interval.
     */
    public long getNextPullDelayMs() {
        long failures = consecutiveFailures.get();
        if (failures == 0) {
            return pullIntervalMs;
        }
        long multiplier = Math.min(1L << failures, MAX_BACKOFF_MULTIPLIER);
        return pullIntervalMs * multiplier;
    }

    public byte[] getLastStats() {
        byte[] snapshot = lastStats;
        return snapshot == null ? null : snapshot.clone();
    }

    public long getConsecutiveFailures() { return consecutiveFailures.get(); }

    /**
     * Functional interface for native stats retrieval.
     * Decouples observability from the native bridge module boundary.
     */
    @FunctionalInterface
    public interface NativeStatsSupplier {
        byte[] getStats(long timeoutMs);
    }
}
