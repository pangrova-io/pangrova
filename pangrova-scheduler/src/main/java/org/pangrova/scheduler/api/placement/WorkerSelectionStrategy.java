package org.pangrova.scheduler.api.placement;

import org.pangrova.common.api.annotation.Internal;

import java.util.Optional;

/**
 * SPI for pluggable worker selection strategies.
 * Implementations decide which worker should execute a given task
 * based on the request context (tenant, data locality, worker state).
 */
@Internal
public interface WorkerSelectionStrategy {

    /**
     * Selects the best worker for the given request.
     *
     * @param request selection context including tenant, file paths, and worker snapshots
     * @return the selected worker snapshot, or empty if no suitable worker is available
     */
    Optional<SchedulingWorkerSnapshot> select(WorkerSelectionRequest request);

    /**
     * Returns the human-readable name of this strategy (e.g. "load-balanced", "fair-share").
     */
    String name();
}
