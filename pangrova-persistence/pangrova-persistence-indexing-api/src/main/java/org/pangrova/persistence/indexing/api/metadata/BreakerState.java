package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Persistent circuit breaker state for distributed index evaluation.
 */
@Experimental
public record BreakerState(
        String indexId,
        int failureCount,
        Instant lastFailureAt,
        Instant lastSuccessAt,
        String state
) {}
