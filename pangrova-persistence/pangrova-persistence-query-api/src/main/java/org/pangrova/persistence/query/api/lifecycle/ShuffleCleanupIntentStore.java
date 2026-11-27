package org.pangrova.persistence.query.api.lifecycle;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.execution.api.ownership.ExecutionAnchor;

/** Durable responsibility to clean up remote Shuffle scopes of a terminal query. */
@Experimental
public interface ShuffleCleanupIntentStore {

    /**
     * Seals new Shuffle admissions before publishing a logical terminal state under the original fresh grant.
     * Stale or recovered grants leave the shared scope to the recovery owner.
     */
    void markPending(ExecutionAnchor grant);

    /**
     * Records convergence under the original fresh grant when there is no durable Shuffle scope.
     * The terminal seal survives until anchor retirement. False requires the remote cleanup protocol.
     * A stale grant leaves the cleanup state for its successor.
     */
    boolean completeWithoutScope(ExecutionAnchor grant);
}
