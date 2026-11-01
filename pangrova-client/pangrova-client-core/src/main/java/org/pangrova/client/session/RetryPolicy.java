package org.pangrova.client.session;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.pangrova.client.error.PangrovaClientException;
import org.pangrova.client.error.PangrovaConnectionException;
import org.pangrova.client.error.PangrovaServerException;

/**
 * Immutable retry policy for transient failure handling in the REST client.
 * Implements exponential backoff with configurable max retries and delays.
 * Both delays must be representable as signed-long milliseconds, the actual
 * jitter and retry-sleep unit; fitting signed-long nanoseconds is not required.
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

    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_BAD_GATEWAY = 502;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;
    private static final int HTTP_GATEWAY_TIMEOUT = 504;

    public RetryPolicy {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0, got " + maxRetries);
        }
        requireMillisecondDelay(initialDelay, "initialDelay");
        requireMillisecondDelay(maxDelay, "maxDelay");
        if (!Double.isFinite(backoffMultiplier) || backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be finite and >= 1.0, got " + backoffMultiplier);
        }
    }

    private static void requireMillisecondDelay(Duration delay, String name) {
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        try {
            if (delay.toMillis() < 0) {
                throw new IllegalArgumentException(name + " must have non-negative milliseconds");
            }
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(name + " must be representable in signed-long milliseconds", overflow);
        }
    }

    /**
     * Computes the delay for the given attempt number (0-indexed).
     *
     * @param attempt the retry attempt number (0 = first retry)
     * @return the delay duration for this attempt
     */
    public Duration delayForAttempt(int attempt) {
        if (initialDelay.compareTo(maxDelay) >= 0) return maxDelay;
        if (attempt <= 0) return initialDelay;
        long delayMs = initialDelay.toMillis();
        long capMs = maxDelay.toMillis();
        for (int i = 0; i < attempt; i++) {
            double next = delayMs * backoffMultiplier;
            if (next >= capMs) return maxDelay;
            long nextMs = (long) next;
            if (nextMs == delayMs) break;
            delayMs = nextMs;
        }
        return Duration.ofMillis(delayMs);
    }

    /**
     * Computes the delay with full jitter for the given attempt number.
     * Returns a random duration in [1ms, delayForAttempt(attempt)] for positive
     * millisecond delays, or zero if no millisecond delay is configured.
     * This randomness spreads retry load; it is not a security primitive.
     *
     * @param attempt the retry attempt number (0 = first retry)
     * @return the jittered delay
     */
    public Duration delayForAttemptWithJitter(int attempt) {
        long baseMs = delayForAttempt(attempt).toMillis();
        if (baseMs <= 1) return Duration.ofMillis(baseMs);
        long jitteredMs = ThreadLocalRandom.current().nextLong(baseMs) + 1;
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
            return isRetryableStatus(serverEx.httpStatusCode());
        }
        return false;
    }

    /**
     * Determines whether an HTTP status code denotes a transient server
     * condition that is safe to retry.
     *
     * @param statusCode the HTTP status code to evaluate
     * @return true when the request may be retried
     */
    public static boolean isRetryableStatus(int statusCode) {
        return statusCode == HTTP_TOO_MANY_REQUESTS
                || statusCode == HTTP_BAD_GATEWAY
                || statusCode == HTTP_SERVICE_UNAVAILABLE
                || statusCode == HTTP_GATEWAY_TIMEOUT;
    }
}
