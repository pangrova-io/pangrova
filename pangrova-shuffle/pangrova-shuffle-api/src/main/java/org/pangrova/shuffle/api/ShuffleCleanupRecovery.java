package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/** Recovery of durable terminal cleanup after Coordinator restart or ownership takeover. */
@Experimental(reason = "Durable cleanup authority may be supported independently of other exchange recovery")
@FunctionalInterface
public interface ShuffleCleanupRecovery {
    /**
     * Reconciles the pending intent under the current fenced execution owner, including when
     * this process never held the original registration. A normal return confirms physical
     * cleanup and durable consumption of the intent. Pending or unknown outcomes must throw
     * and remain retryable.
     */
    void recoverCleanupShuffle(String queryId);
}
