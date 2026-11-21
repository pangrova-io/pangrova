package org.pangrova.common.resilience.policy;

import java.time.Duration;

/**
 * Immutable per-service resilience policy configuration, loadable from YAML.
 *
 * <p>Each service (e.g., "coordinator-to-metadata", "coordinator-to-worker") can have
 * independent circuit breaker, retry, bulkhead, rate limiter, and timeout settings.</p>
 *
 * @param serviceName    logical name of the service
 * @param circuitBreaker circuit breaker settings (null if disabled)
 * @param retry          retry policy settings (null if disabled)
 * @param bulkhead       bulkhead settings (null if disabled)
 * @param rateLimiter    rate limiter settings (null if disabled)
 * @param timeLimiter    time limiter settings (null if disabled)
 */
public record ResiliencePolicyConfig(
        String serviceName,
        CircuitBreakerSettings circuitBreaker,
        RetrySettings retry,
        BulkheadSettings bulkhead,
        RateLimiterSettings rateLimiter,
        TimeLimiterSettings timeLimiter) {

    private static final long DEFAULT_CIRCUIT_BREAKER_WAIT_SECONDS = 30;

    /**
     * Circuit breaker configuration.
     *
     * @param failureRateThreshold     failure rate percentage (0–100) that triggers OPEN
     * @param slowCallRateThreshold    slow call rate percentage (0–100) that triggers OPEN
     * @param slowCallDurationMs       duration in ms above which a call is considered slow
     * @param minimumNumberOfCalls     minimum calls before failure rate is evaluated
     * @param slidingWindowSize        number of calls in the sliding window
     * @param waitDurationInOpenStateMs time in OPEN before transitioning to HALF_OPEN
     * @param permittedNumberOfCallsInHalfOpenState max probe calls in HALF_OPEN
     */
    public record CircuitBreakerSettings(
            float failureRateThreshold,
            float slowCallRateThreshold,
            long slowCallDurationMs,
            int minimumNumberOfCalls,
            int slidingWindowSize,
            long waitDurationInOpenStateMs,
            int permittedNumberOfCallsInHalfOpenState) {

        private static final float DEFAULT_FAILURE_RATE_THRESHOLD = 50.0f;
        private static final long DEFAULT_SLOW_CALL_DURATION_MS = 60_000;
        private static final long DEFAULT_WAIT_DURATION_IN_OPEN_STATE_MS = 30_000;

        public static CircuitBreakerSettings defaults() {
            return new CircuitBreakerSettings(DEFAULT_FAILURE_RATE_THRESHOLD, 100.0f,
                    DEFAULT_SLOW_CALL_DURATION_MS, 10, 100, DEFAULT_WAIT_DURATION_IN_OPEN_STATE_MS, 3);
        }
    }

    /**
     * Retry configuration.
     *
     * @param maxAttempts        maximum number of attempts (including initial call)
     * @param waitDurationMs     initial wait between retries
     * @param backoffMultiplier  exponential backoff multiplier (1.0 = no backoff)
     * @param maxWaitDurationMs  maximum wait duration cap
     */
    public record RetrySettings(
            int maxAttempts,
            long waitDurationMs,
            double backoffMultiplier,
            long maxWaitDurationMs) {

        private static final long DEFAULT_WAIT_DURATION_MS = 500;
        private static final long DEFAULT_MAX_WAIT_DURATION_MS = 10_000;

        public static RetrySettings defaults() {
            return new RetrySettings(3, DEFAULT_WAIT_DURATION_MS, 2.0, DEFAULT_MAX_WAIT_DURATION_MS);
        }
    }

    /**
     * Bulkhead (concurrency isolation) configuration.
     *
     * @param maxConcurrentCalls maximum concurrent executions
     * @param maxWaitDurationMs  maximum time to wait for a permit
     */
    public record BulkheadSettings(int maxConcurrentCalls, long maxWaitDurationMs) {

        private static final int DEFAULT_MAX_CONCURRENT_CALLS = 25;
        private static final long DEFAULT_MAX_WAIT_DURATION_MS = 500;

        public static BulkheadSettings defaults() {
            return new BulkheadSettings(DEFAULT_MAX_CONCURRENT_CALLS, DEFAULT_MAX_WAIT_DURATION_MS);
        }
    }

    /**
     * Rate limiter configuration.
     *
     * @param limitForPeriod       number of permits per refresh period
     * @param limitRefreshPeriodMs refresh period in milliseconds
     * @param timeoutDurationMs    max wait time for a permit
     */
    public record RateLimiterSettings(int limitForPeriod, long limitRefreshPeriodMs, long timeoutDurationMs) {

        private static final long DEFAULT_TIMEOUT_DURATION_MS = 500;

        public static RateLimiterSettings defaults() {
            return new RateLimiterSettings(100, 1_000, DEFAULT_TIMEOUT_DURATION_MS);
        }
    }

    /**
     * Time limiter configuration.
     *
     * @param timeoutDurationMs maximum duration before timeout
     * @param cancelRunningFuture whether to cancel the running future on timeout
     */
    public record TimeLimiterSettings(long timeoutDurationMs, boolean cancelRunningFuture) {

        private static final long DEFAULT_TIMEOUT_DURATION_MS = 30_000;

        public static TimeLimiterSettings defaults() {
            return new TimeLimiterSettings(DEFAULT_TIMEOUT_DURATION_MS, true);
        }
    }

    /** Creates a config with all defaults enabled for the given service. */
    public static ResiliencePolicyConfig allDefaults(String serviceName) {
        return new ResiliencePolicyConfig(
                serviceName,
                CircuitBreakerSettings.defaults(),
                RetrySettings.defaults(),
                BulkheadSettings.defaults(),
                RateLimiterSettings.defaults(),
                TimeLimiterSettings.defaults());
    }

    /** Converts circuit breaker wait duration to Duration. */
    public Duration circuitBreakerWaitDuration() {
        return circuitBreaker != null
                ? Duration.ofMillis(circuitBreaker.waitDurationInOpenStateMs())
                : Duration.ofSeconds(DEFAULT_CIRCUIT_BREAKER_WAIT_SECONDS);
    }
}
