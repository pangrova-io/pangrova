package org.pangrova.coordinator.streaming;

/**
 * System-level configuration for CDC ingestion streams.
 *
 * <p>Immutable record with validation — all values must be positive.
 * Use {@link #withDefaults()} for sensible production defaults.</p>
 *
 * @param maxStreamsPerTenant           maximum concurrent RUNNING/CREATING streams per tenant
 * @param defaultBatchSize             default poll batch size
 * @param defaultFlushInterval         default flush interval (ISO-8601 duration string, e.g. "PT5S")
 * @param defaultMaxConsecutiveFailures default failure threshold before FAILED state
 * @param defaultMaxConcurrentWrites   default max concurrent writes per stream
 * @param defaultMaxPendingBytes       default max pending bytes per stream
 * @param pipelineThreadPoolSize       thread pool size for CDC pipeline threads
 */
public record StreamConfig(
        int maxStreamsPerTenant,
        int defaultBatchSize,
        String defaultFlushInterval,
        int defaultMaxConsecutiveFailures,
        int defaultMaxConcurrentWrites,
        long defaultMaxPendingBytes,
        int pipelineThreadPoolSize
) {

    public StreamConfig {
        if (maxStreamsPerTenant <= 0) {
            throw new IllegalArgumentException("maxStreamsPerTenant must be positive");
        }
        if (defaultBatchSize <= 0) {
            throw new IllegalArgumentException("defaultBatchSize must be positive");
        }
        if (defaultFlushInterval == null || defaultFlushInterval.isBlank()) {
            throw new IllegalArgumentException("defaultFlushInterval must not be blank");
        }
        if (defaultMaxConsecutiveFailures <= 0) {
            throw new IllegalArgumentException("defaultMaxConsecutiveFailures must be positive");
        }
        if (defaultMaxConcurrentWrites <= 0) {
            throw new IllegalArgumentException("defaultMaxConcurrentWrites must be positive");
        }
        if (defaultMaxPendingBytes <= 0) {
            throw new IllegalArgumentException("defaultMaxPendingBytes must be positive");
        }
        if (pipelineThreadPoolSize <= 0) {
            throw new IllegalArgumentException("pipelineThreadPoolSize must be positive");
        }
    }

    /** Returns a configuration with sensible production defaults. */
    public static StreamConfig withDefaults() {
        return new StreamConfig(
                10,          // maxStreamsPerTenant
                1000,        // defaultBatchSize
                "PT5S",      // defaultFlushInterval (5 seconds)
                10,          // defaultMaxConsecutiveFailures
                4,           // defaultMaxConcurrentWrites
                64 * 1024 * 1024L, // defaultMaxPendingBytes (64 MB)
                8            // pipelineThreadPoolSize
        );
    }
}
