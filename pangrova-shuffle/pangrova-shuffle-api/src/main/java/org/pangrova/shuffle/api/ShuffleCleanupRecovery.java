package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/** Recovery of durable terminal cleanup after Coordinator restart or ownership takeover. */
@Experimental(reason = "Durable cleanup authority may be supported independently of other exchange recovery")
@FunctionalInterface
public interface ShuffleCleanupRecovery {
    /**
     * Reconciles the pending intent under the current fenced execution owner, including when
     * this process never held the original registration. Normal return permits the caller to
     * confirm completion in the authority store. An unknown outcome must remain retryable.
     */
    void recoverCleanupShuffle(String queryId);
}
