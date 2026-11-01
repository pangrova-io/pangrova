package org.pangrova.indexing.api;

import java.time.Instant;

/**
 * Persistent circuit breaker state for distributed index evaluation.
 */
public record BreakerState(
        String indexId,
        int failureCount,
        Instant lastFailureAt,
        Instant lastSuccessAt,
        String state
) {}
