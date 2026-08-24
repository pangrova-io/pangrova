package org.pangrova.indexing.eval;

/**
 * Simplified projection of distributed circuit breaker state.
 *
 * <p>Used by {@link IndexEvalCircuitBreaker} to check shared breaker status
 * without depending on the metadata module's full breaker record.</p>
 *
 * @param failureCount      shared failure count across all nodes
 * @param openUntilEpochMs  epoch millis until which the breaker is open (0 = closed)
 */
public record SharedBreakerState(int failureCount, long openUntilEpochMs) {}
