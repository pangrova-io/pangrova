package org.pangrova.materialization.domain;

import java.time.Instant;

/**
 * Proof that a command still owns the execution right it was admitted with.
 *
 * <p>The fence binds the coordinator execution anchor, the leader control
 * epoch, and the lease deadline. Repository writes validate all three; an
 * expired lease or stale epoch rejects publication even when the data-plane
 * commit succeeded, and the candidate is retained for reconciliation.
 *
 * @param executionAnchor coordinator-issued execution anchor identity
 * @param controlEpoch coordinator leader control epoch
 * @param leaseExpiresAt deadline after which the fence is void
 */
public record ExecutionFence(String executionAnchor, long controlEpoch, Instant leaseExpiresAt) {

    public ExecutionFence {
        executionAnchor = Require.text(executionAnchor, "executionAnchor");
        if (controlEpoch < 0) {
            throw new IllegalArgumentException("controlEpoch must not be negative");
        }
        if (leaseExpiresAt == null) {
            throw new IllegalArgumentException("leaseExpiresAt must not be null");
        }
    }

    /**
     * Returns whether the fence has expired at the given instant.
     *
     * @param now current instant
     * @return true when the lease deadline is at or before {@code now}
     */
    public boolean isExpiredAt(Instant now) {
        return !leaseExpiresAt.isAfter(now);
    }

    /**
     * Rejects an expired fence.
     *
     * @param now current instant
     * @throws MaterializationException when the fence is no longer valid
     */
    public void requireValidAt(Instant now) {
        if (isExpiredAt(now)) {
            throw MaterializationException.fenceRejected(
                    "execution fence expired at " + leaseExpiresAt + " for anchor " + executionAnchor);
        }
    }
}
