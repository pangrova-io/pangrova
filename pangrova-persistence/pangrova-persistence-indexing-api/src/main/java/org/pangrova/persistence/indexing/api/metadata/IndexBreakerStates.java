package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.time.Instant;
import java.util.Optional;

/** Shared evaluation circuit-breaker observations. */
@Experimental
public interface IndexBreakerStates {
    /** Gets the circuit breaker state for an index. */
    default Optional<BreakerState> getBreakerState(String indexId) {
        throw new UnsupportedOperationException("Shared index breaker state unavailable");
    }

    /** Updates the circuit breaker state. Uses upsert semantics. */
    default void updateBreakerState(String indexId, int failureCount,
                                    Instant lastFailureAt, Instant lastSuccessAt, String state) {
        throw new UnsupportedOperationException("Shared index breaker state unavailable");
    }
}
