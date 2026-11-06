/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.indexing;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.pangrova.indexing.eval.BreakerStateStore;
import org.pangrova.indexing.eval.SharedBreakerState;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.BreakerState;

/**
 * Coordinator-side {@link BreakerStateStore} that projects the authoritative
 * distributed circuit-breaker state persisted by {@link IndexMetadataStore}
 * (table {@code index_eval_breaker_state}) into the indexing module's
 * dependency-free {@link SharedBreakerState} view.
 *
 * <p><b>F-IDX-BREAKER.</b> The index-eval circuit breaker was previously wired
 * with {@link BreakerStateStore#noOp()}, so each coordinator tracked failures
 * only locally and could never fast-open cluster-wide: a repeatedly failing
 * index would keep being probed on every node. This adapter replicates the
 * breaker's open decision ({@code failureThreshold} + {@code openDuration})
 * into the shared metadata store so every coordinator observes the OPEN window
 * and stops evaluating a known-bad index.</p>
 *
 * <p>Degradation: when the injected {@link IndexMetadataStore} is not
 * JDBC-backed its default {@code getBreakerState}/{@code updateBreakerState}
 * methods are empty no-ops, so this adapter transparently behaves like
 * {@link BreakerStateStore#noOp()} — matching the prior local-only behaviour
 * without any special casing.</p>
 *
 * <p>The distributed failure count is best-effort (read-modify-write via upsert
 * without a cross-node lock); a small over/under count is acceptable for a
 * circuit breaker. The safety invariant — once a node opens the breaker, all
 * nodes observe {@code openUntilEpochMs} and stop probing — holds because the
 * OPEN state and its deadline are derived deterministically from the shared
 * {@code failureCount}/{@code lastFailureAt} + {@code openDuration}.</p>
 *
 * @see org.pangrova.indexing.eval.IndexEvalCircuitBreaker
 */
public final class MetadataBreakerStateStore implements BreakerStateStore {

    private static final String STATE_OPEN = "OPEN";
    private static final String STATE_CLOSED = "CLOSED";

    private final IndexMetadataStore metadataStore;
    private final int failureThreshold;
    private final long openDurationMs;

    /**
     * @param metadataStore    authoritative breaker-state store (must not be null)
     * @param failureThreshold consecutive failures before the circuit opens (>= 1);
     *                         must match the local {@code IndexEvalCircuitBreaker}
     * @param openDuration     how long the circuit stays open after tripping (positive);
     *                         must match the local {@code IndexEvalCircuitBreaker}
     */
    public MetadataBreakerStateStore(IndexMetadataStore metadataStore,
                                     int failureThreshold, Duration openDuration) {
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore must not be null");
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1, got " + failureThreshold);
        }
        if (openDuration == null || openDuration.isNegative() || openDuration.isZero()) {
            throw new IllegalArgumentException("openDuration must be positive");
        }
        this.failureThreshold = failureThreshold;
        this.openDurationMs = openDuration.toMillis();
    }

    @Override
    public Optional<SharedBreakerState> getState(String indexId) {
        return metadataStore.getBreakerState(indexId).map(this::toShared);
    }

    private SharedBreakerState toShared(BreakerState state) {
        long openUntilEpochMs = 0L;
        if (STATE_OPEN.equals(state.state()) && state.lastFailureAt() != null) {
            openUntilEpochMs = state.lastFailureAt().toEpochMilli() + openDurationMs;
        }
        return new SharedBreakerState(state.failureCount(), openUntilEpochMs);
    }

    @Override
    public void recordFailure(String indexId, Instant failedAt) {
        Optional<BreakerState> current = metadataStore.getBreakerState(indexId);
        int newCount = current.map(BreakerState::failureCount).orElse(0) + 1;
        Instant lastSuccessAt = current.map(BreakerState::lastSuccessAt).orElse(null);
        String state = newCount >= failureThreshold ? STATE_OPEN : STATE_CLOSED;
        metadataStore.updateBreakerState(indexId, newCount, failedAt, lastSuccessAt, state);
    }

    @Override
    public void recordSuccess(String indexId) {
        Instant lastFailureAt = metadataStore.getBreakerState(indexId)
                .map(BreakerState::lastFailureAt).orElse(null);
        metadataStore.updateBreakerState(indexId, 0, lastFailureAt, Instant.now(), STATE_CLOSED);
    }
}
