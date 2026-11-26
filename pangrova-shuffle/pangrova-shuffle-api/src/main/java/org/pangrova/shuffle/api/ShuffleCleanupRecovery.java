package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/** Recovery of durable terminal cleanup after Coordinator restart or ownership takeover. */
@Experimental(reason = "Durable cleanup authority may be supported independently of other exchange recovery")
public interface ShuffleCleanupRecovery {
    /**
     * Cleans a locally tracked scope under the original grant, using its cached capability when usable.
     * False means this process has no tracked scope. Failures and ambiguous outcomes throw.
     */
    boolean cleanupTrackedShuffle(String queryId, ShuffleExecutionOwner owner);

    /**
     * Reconciles the pending intent under the current fenced execution owner, including when
     * this process never held the original registration. A normal return confirms physical
     * cleanup and durable consumption of the intent. Pending or unknown outcomes must throw
     * and remain retryable.
     */
    void recoverCleanupShuffle(String queryId, ShuffleExecutionOwner owner);
}
