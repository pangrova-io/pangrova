package org.pangrova.client.rest;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Immutable retry policy for transient failure handling in the REST client.
 * Implements exponential backoff with configurable max retries and delays.
 *
 * @param maxRetries       maximum number of retry attempts (0 = no retries)
 * @param initialDelay     initial delay before first retry
 * @param maxDelay         maximum delay between retries (backoff cap)
 * @param backoffMultiplier multiplier applied to delay after each retry (typically 2.0)
 */
public record RetryPolicy(
        int maxRetries,
        Duration initialDelay,
        Duration maxDelay,
        double backoffMultiplier
) {

    /** Default policy: 3 retries, 1s initial, 30s max, 2x backoff. */
    public static final RetryPolicy DEFAULT = new RetryPolicy(3,
            Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0);

    /** No-retry policy for operations that should not be retried. */
    public static final RetryPolicy NONE = new RetryPolicy(0,
            Duration.ZERO, Duration.ZERO, 1.0);

    public RetryPolicy {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0, got " + maxRetries);
        }
        if (initialDelay == null || initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must be non-negative");
        }
        if (maxDelay == null || maxDelay.isNegative()) {
            throw new IllegalArgumentException("maxDelay must be non-negative");
        }
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1.0, got " + backoffMultiplier);
        }
    }

    /**
     * Computes the delay for the given attempt number (0-indexed).
     *
     * @param attempt the retry attempt number (0 = first retry)
     * @return the delay duration for this attempt
     */
    public Duration delayForAttempt(int attempt) {
        if (attempt <= 0) {
            return initialDelay;
        }
        long delayMs = initialDelay.toMillis();
        for (int i = 0; i < attempt; i++) {
            delayMs = (long) (delayMs * backoffMultiplier);
            if (delayMs >= maxDelay.toMillis()) {
                return maxDelay;
            }
        }
        return Duration.ofMillis(delayMs);
    }

    /**
     * Computes the delay with full jitter for the given attempt number.
     * Returns a random duration in [1ms, delayForAttempt(attempt)].
     */
    public Duration delayForAttemptWithJitter(int attempt) {
        long baseMs = delayForAttempt(attempt).toMillis();
        if (baseMs <= 1) {
            return Duration.ofMillis(1);
        }
        long jitteredMs = ThreadLocalRandom.current().nextLong(1, baseMs + 1);
        return Duration.ofMillis(jitteredMs);
    }

    /**
     * Determines if the given exception represents a transient failure
     * that should be retried.
     *
     * @param exception the exception to evaluate
     * @return true if the operation should be retried
     */
    public static boolean isRetryable(PangrovaClientException exception) {
        if (exception instanceof PangrovaConnectionException) {
            return true;
        }
        if (exception instanceof PangrovaServerException serverEx) {
            return HttpStatusCodes.isRetryable(serverEx.httpStatusCode());
        }
        return false;
    }
}
