package org.pangrova.worker.task;

/**
 * Configuration for task retry on transient failures.
 *
 * @param maxRetries     maximum number of retry attempts before permanent failure (default 3)
 * @param initialBackoffMs initial backoff delay in milliseconds (default 1000)
 * @param backoffMultiplier multiplier applied to backoff on each retry (default 2.0)
 * @param maxBackoffMs   maximum backoff delay in milliseconds (default 30000)
 */
public record TaskRetryConfig(
        int maxRetries,
        long initialBackoffMs,
        double backoffMultiplier,
        long maxBackoffMs) {

    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_INITIAL_BACKOFF_MS = 1000;
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
    public static final long DEFAULT_MAX_BACKOFF_MS = 30_000;

    public TaskRetryConfig {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative: " + maxRetries);
        }
        if (initialBackoffMs <= 0) {
            throw new IllegalArgumentException("initialBackoffMs must be positive: " + initialBackoffMs);
        }
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1.0: " + backoffMultiplier);
        }
        if (maxBackoffMs < initialBackoffMs) {
            throw new IllegalArgumentException(
                    "maxBackoffMs must be >= initialBackoffMs: " + maxBackoffMs + " < " + initialBackoffMs);
        }
    }

    /** Returns a retry configuration populated with the default values. */
    public static TaskRetryConfig defaults() {
        return new TaskRetryConfig(
                DEFAULT_MAX_RETRIES,
                DEFAULT_INITIAL_BACKOFF_MS,
                DEFAULT_BACKOFF_MULTIPLIER,
                DEFAULT_MAX_BACKOFF_MS);
    }

    /**
     * Computes the backoff delay for the given attempt number (0-based).
     */
    public long computeBackoffMs(int attempt) {
        long delay = (long) (initialBackoffMs * Math.pow(backoffMultiplier, attempt));
        return Math.min(delay, maxBackoffMs);
    }
}
