package org.pangrova.coordinator.dynamictable;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for Dynamic Table incremental refresh.
 *
 * <p>All numeric fields must be positive. {@code defaultTargetLag} must not be null.
 * Use {@link #defaults()} to obtain a configuration with sensible defaults.</p>
 *
 * @param maxConcurrentGlobal        global max concurrent refresh tasks (default 4, R16 AC2)
 * @param maxConcurrentPerTenant     per-tenant max concurrent refresh tasks (default 2, R9 AC4)
 * @param maxDynamicTablesPerTenant  per-tenant Dynamic Table quota (default 100, R9 AC4)
 * @param defaultTargetLag           default data freshness SLA (default PT5M, R5)
 * @param microBatchIntervalMs       CDC micro-batch aggregation interval in ms (default 1000, R4 AC5)
 * @param retryInitialIntervalMs     exponential backoff initial interval in ms (default 1000, R7 AC4)
 * @param retryMaxIntervalMs         exponential backoff max interval in ms (default 300000, R7 AC4)
 * @param retryMaxAttempts           max retry attempts before giving up (default 5, R7 AC4)
 * @param autoSuspendThreshold       consecutive failures before auto-suspend (default 10, R7 AC5)
 * @param lockTimeoutMultiplier      lock timeout = targetLag * multiplier (default 3.0, R21 AC3)
 * @param cdcFallbackThresholdMs     CDC unavailability threshold before SNAPSHOT fallback in ms (default 300000, R20 AC4)
 * @param historyRetentionCount      max refresh history entries to retain (default 1000, R23 AC3)
 * @param historyRetentionDays       max refresh history retention in days (default 30, R23 AC3)
 * @param queueOverflowThreshold     queue depth threshold for overflow alert (default 20, R16 AC4)
 */
public record DynamicTableConfig(
        int maxConcurrentGlobal,
        int maxConcurrentPerTenant,
        int maxDynamicTablesPerTenant,
        Duration defaultTargetLag,
        long microBatchIntervalMs,
        long retryInitialIntervalMs,
        long retryMaxIntervalMs,
        int retryMaxAttempts,
        int autoSuspendThreshold,
        double lockTimeoutMultiplier,
        long cdcFallbackThresholdMs,
        int historyRetentionCount,
        int historyRetentionDays,
        int queueOverflowThreshold
) {

    public DynamicTableConfig {
        Objects.requireNonNull(defaultTargetLag, "defaultTargetLag must not be null");
        requirePositive(maxConcurrentGlobal, "maxConcurrentGlobal");
        requirePositive(maxConcurrentPerTenant, "maxConcurrentPerTenant");
        requirePositive(maxDynamicTablesPerTenant, "maxDynamicTablesPerTenant");
        requirePositive(microBatchIntervalMs, "microBatchIntervalMs");
        requirePositive(retryInitialIntervalMs, "retryInitialIntervalMs");
        requirePositive(retryMaxIntervalMs, "retryMaxIntervalMs");
        requirePositive(retryMaxAttempts, "retryMaxAttempts");
        requirePositive(autoSuspendThreshold, "autoSuspendThreshold");
        requirePositive(lockTimeoutMultiplier, "lockTimeoutMultiplier");
        requirePositive(cdcFallbackThresholdMs, "cdcFallbackThresholdMs");
        requirePositive(historyRetentionCount, "historyRetentionCount");
        requirePositive(historyRetentionDays, "historyRetentionDays");
        requirePositive(queueOverflowThreshold, "queueOverflowThreshold");
    }

    /** Return a configuration with sensible defaults for all fields. */
    public static DynamicTableConfig defaults() {
        return new DynamicTableConfig(
                4,                          // maxConcurrentGlobal
                2,                          // maxConcurrentPerTenant
                100,                        // maxDynamicTablesPerTenant
                Duration.ofMinutes(5),      // defaultTargetLag = PT5M
                1_000L,                     // microBatchIntervalMs
                1_000L,                     // retryInitialIntervalMs
                300_000L,                   // retryMaxIntervalMs
                5,                          // retryMaxAttempts
                10,                         // autoSuspendThreshold
                3.0,                        // lockTimeoutMultiplier
                300_000L,                   // cdcFallbackThresholdMs
                1_000,                      // historyRetentionCount
                30,                         // historyRetentionDays
                20                          // queueOverflowThreshold
        );
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
    }

    private static void requirePositive(double value, String name) {
        if (value <= 0.0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
    }
}
