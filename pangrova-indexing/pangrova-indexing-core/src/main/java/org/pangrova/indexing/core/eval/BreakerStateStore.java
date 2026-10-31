package org.pangrova.indexing.core.eval;

import java.time.Instant;
import java.util.Optional;

/**
 * Functional interface for distributed circuit breaker state persistence.
 *
 * <p>Implementations are provided by the coordinator layer which has access
 * to the metadata store. The indexing module uses this abstraction to avoid
 * a direct dependency on the metadata module.</p>
 *
 * @see IndexEvalCircuitBreaker
 */
public interface BreakerStateStore {

    /** Sentinel instance returned by {@link #noOp()}. Used for identity comparison. */
    BreakerStateStore NO_OP = new BreakerStateStore() {
        @Override
        public Optional<SharedBreakerState> getState(String indexId) {
            return Optional.empty();
        }

        @Override
        public void recordFailure(String indexId, Instant failedAt) {}

        @Override
        public void recordSuccess(String indexId) {}
    };

    /**
     * Reads the shared breaker state for the given index.
     *
     * @param indexId the index identifier
     * @return shared state if present, empty otherwise
     */
    Optional<SharedBreakerState> getState(String indexId);

    /**
     * Records a failure in the shared store, incrementing the failure count.
     *
     * @param indexId  the index identifier
     * @param failedAt timestamp of the failure
     */
    void recordFailure(String indexId, Instant failedAt);

    /**
     * Records a success in the shared store, resetting the failure count.
     *
     * @param indexId the index identifier
     */
    void recordSuccess(String indexId);

    /**
     * Returns a no-op implementation that always returns empty state
     * and performs no writes. Used when no shared store is configured.
     */
    static BreakerStateStore noOp() {
        return NO_OP;
    }
}
