package org.pangrova.scheduler.api.placement;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/**
 * Coordinator-local, thread-safe worker ranking. Implementations must perform
 * bounded in-memory work, without network calls, retained requests or resources.
 * The returned identity must belong to this request. Empty means no selection;
 * exceptions propagate without fallback to a different strategy.
 *
 * <p>Selection is advisory, not a reservation. Concurrent calls may select the
 * same worker. Calls are not idempotent: a strategy may rotate equal candidates.
 * Cancellation and admission remain the caller's responsibility; this synchronous
 * computation does not own tasks, workers, threads or their lifecycles.</p>
 */
@Experimental
public interface WorkerSelectionStrategy {
    Optional<String> select(WorkerSelectionRequest request);
}
