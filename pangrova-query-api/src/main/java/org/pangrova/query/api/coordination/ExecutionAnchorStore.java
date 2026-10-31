package org.pangrova.query.api.coordination;

import org.pangrova.common.api.annotation.Stable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Durable, fenced ownership store for queries and background executions.
 *
 * <p>Implementations must use one authoritative backend clock for acquisition,
 * renewal and expiry. Tokens must come from a durable never-reused sequence so
 * a reused identifier cannot revive an older owner after row cleanup. Claim
 * and renewal operations are bounded or batched to avoid one coordination-store
 * lease per task.</p>
 */
@Stable(since = "1.0")
public interface ExecutionAnchorStore {

    /**
     * Acquires an inactive or expired execution and allocates its next token.
     *
     * @return the acquired anchor, or empty while another live owner holds it
     */
    Optional<ExecutionAnchor> tryAcquire(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration);

    /**
     * Renews every live anchor held by one exact process incarnation in one batch.
     *
     * <p>The immutable receipt contains the exact execution identity, process,
     * fencing token and backend-clock deadline committed by this batch. Counts
     * alone cannot establish ownership while executions are acquired or released
     * concurrently. Expired anchors must never be revived.</p>
     *
     * @return exact anchors renewed before their existing deadline
     */
    List<ExecutionAnchor> renewOwned(
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration);

    /** Marks an execution complete only for the exact current fenced owner. */
    boolean release(ExecutionAnchor anchor);

    /** Makes one exact fenced execution immediately eligible for takeover. */
    boolean yield(ExecutionAnchor anchor);

    /** Makes all live anchors for one process immediately eligible for takeover. */
    int yieldOwned(String ownerId, String ownerIncarnation);

    /**
     * Atomically takes over at most {@code maxBatchSize} expired live executions,
     * allocating a new token for every claimed execution.
     */
    List<ExecutionAnchor> claimExpired(
            String newOwnerId,
            String newOwnerIncarnation,
            Duration leaseDuration,
            int maxBatchSize);

    /** Returns the unexpired live anchor for an execution, if present. */
    Optional<ExecutionAnchor> current(String executionId);
}
