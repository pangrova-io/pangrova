package org.pangrova.coordinator.speculation;

import java.util.Map;

/**
 * Configuration for speculative execution with per-query-type thresholds.
 * Immutable record with sensible defaults.
 *
 * @param enabled                whether speculative execution is active (default: false)
 * @param detectionIntervalMs    interval between straggler detection sweeps in milliseconds (default: 5000)
 * @param progressRateEnabled    whether progress-rate-based detection is active (default: true)
 * @param dataSkewEnabled        whether data skew triggers speculation (default: true)
 * @param dataSkewMultiplier     input size ratio above median to consider skewed (default: 3.0)
 * @param queryTypeThresholds    per-query-type median multiplier overrides (e.g., "SCAN" → 1.2)
 * @param defaultMedianMultiplier default median multiplier when no query-type override exists (default: 1.5)
 * @param singleTaskDurationThresholdMs explicit duration threshold for a one-task stage; zero disables it
 */
public record SpeculationConfig(
        boolean enabled,
        long detectionIntervalMs,
        boolean progressRateEnabled,
        boolean dataSkewEnabled,
        double dataSkewMultiplier,
        Map<String, Double> queryTypeThresholds,
        double defaultMedianMultiplier,
        long singleTaskDurationThresholdMs) {

    /** Default detection interval: 5 seconds. */
    public static final long DEFAULT_DETECTION_INTERVAL_MS = 5_000L;

    /** Default median multiplier for elapsed-time-based detection. */
    public static final double DEFAULT_MEDIAN_MULTIPLIER = 1.5;

    /** Default data skew multiplier (input bytes ratio above median). */
    public static final double DEFAULT_DATA_SKEW_MULTIPLIER = 3.0;

    /** Disabled configuration singleton. */
    public static final SpeculationConfig DISABLED = new SpeculationConfig(
            false, DEFAULT_DETECTION_INTERVAL_MS, true, true,
            DEFAULT_DATA_SKEW_MULTIPLIER, Map.of(), DEFAULT_MEDIAN_MULTIPLIER, 0L);

    public SpeculationConfig {
        if (detectionIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "detectionIntervalMs must be positive, got " + detectionIntervalMs);
        }
        if (dataSkewMultiplier <= 0) {
            throw new IllegalArgumentException(
                    "dataSkewMultiplier must be positive, got " + dataSkewMultiplier);
        }
        if (defaultMedianMultiplier <= 0) {
            throw new IllegalArgumentException(
                    "defaultMedianMultiplier must be positive, got " + defaultMedianMultiplier);
        }
        if (singleTaskDurationThresholdMs < 0) {
            throw new IllegalArgumentException("singleTaskDurationThresholdMs must not be negative");
        }
        queryTypeThresholds = Map.copyOf(queryTypeThresholds);
    }

    /** Preserves the peer-based policy for existing callers. */
    public SpeculationConfig(boolean enabled, long detectionIntervalMs,
            boolean progressRateEnabled, boolean dataSkewEnabled,
            double dataSkewMultiplier, Map<String, Double> queryTypeThresholds,
            double defaultMedianMultiplier) {
        this(enabled, detectionIntervalMs, progressRateEnabled, dataSkewEnabled,
                dataSkewMultiplier, queryTypeThresholds, defaultMedianMultiplier, 0L);
    }

    /**
     * Backward-compatible constructor for existing callers.
     */
    public SpeculationConfig(boolean enabled, long detectionIntervalMs) {
        this(enabled, detectionIntervalMs, true, true,
                DEFAULT_DATA_SKEW_MULTIPLIER, Map.of(), DEFAULT_MEDIAN_MULTIPLIER, 0L);
    }

    /**
     * Returns the median multiplier for the given query type, falling back to the default.
     */
    public double medianMultiplierForQueryType(String queryType) {
        if (queryType == null || queryTypeThresholds.isEmpty()) {
            return defaultMedianMultiplier;
        }
        return queryTypeThresholds.getOrDefault(queryType, defaultMedianMultiplier);
    }
}
