package org.pangrova.resilience.breaker;

import java.time.Clock;

/**
 * Immutable configuration for {@link CircuitBreaker}.
 * Groups all tuning parameters into a single object to keep the
 * CircuitBreaker constructor within the ≤4 param governance limit.
 *
 * @param failureRateThreshold failure rate (0.0–1.0) that triggers OPEN state
 * @param minimumCalls         minimum calls before failure rate is evaluated
 * @param waitDurationMs       time in OPEN state before transitioning to HALF_OPEN
 * @param halfOpenMaxCalls     max probe calls allowed in HALF_OPEN state
 * @param slidingWindowSize    number of most recent calls tracked by the sliding window
 * @param clock                clock for time-based transitions
 */
public record CircuitBreakerConfig(
        double failureRateThreshold,
        int minimumCalls,
        long waitDurationMs,
        int halfOpenMaxCalls,
        int slidingWindowSize,
        Clock clock) {

    private static final double DEFAULT_FAILURE_RATE_THRESHOLD = 0.5;
    private static final long DEFAULT_WAIT_DURATION_MS = 30_000;

    /** Default configuration: 50% failure rate, 10 min calls, 30s wait, 3 half-open probes, 100 window. */
    public static CircuitBreakerConfig defaults() {
        return new CircuitBreakerConfig(DEFAULT_FAILURE_RATE_THRESHOLD, 10, DEFAULT_WAIT_DURATION_MS, 3, 100,
                Clock.systemUTC());
    }

    /**
     * Convenience constructor without slidingWindowSize (defaults to 100).
     */
    public CircuitBreakerConfig(double failureRateThreshold, int minimumCalls,
                                long waitDurationMs, int halfOpenMaxCalls, Clock clock) {
        this(failureRateThreshold, minimumCalls, waitDurationMs, halfOpenMaxCalls, 100, clock);
    }
}
