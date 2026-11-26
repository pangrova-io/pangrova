package org.pangrova.persistence.query.api.coordination;

import org.pangrova.execution.api.ownership.ExecutionAnchor;

import org.pangrova.common.api.annotation.Stable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

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
     * @return the acquired anchor, or empty while a live owner or its durable read window protects it
     */
    Optional<ExecutionAnchor> tryAcquire(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration);

    /** One committed SQL execution grant and its observed tenant publication revision. */
    record QueryAcquisition(ExecutionAnchor anchor, OptionalLong publicationRevision) {
        public QueryAcquisition {
            java.util.Objects.requireNonNull(anchor, "anchor");
            java.util.Objects.requireNonNull(publicationRevision, "publicationRevision");
        }
    }

    /**
     * Acquires a new SQL execution and samples the tenant's publication revision with the grant.
     * Backends without a publication schema return an acquisition without a revision.
     */
    default Optional<QueryAcquisition> tryAcquireQuery(String executionId, String ownerId,
            String ownerIncarnation, Duration leaseDuration, String tenantId) {
        return tryAcquire(executionId, ownerId, ownerIncarnation, leaseDuration)
                .map(anchor -> new QueryAcquisition(anchor, OptionalLong.empty()));
    }

    /**
     * Acquires a fresh query, reserves its tenant slot, and samples publication visibility in one transaction.
     * @throws TenantCapacityExceeded if the tenant has no available query slot
     */
    default Optional<QueryAcquisition> tryAcquireLimitedQuery(String executionId, String ownerId,
            String ownerIncarnation, Duration leaseDuration, String tenantId, int maxConcurrentQueries) {
        throw new UnsupportedOperationException("Atomic durable query admission is unavailable");
    }

    /** The durable tenant capacity gate denied the candidate. */
    final class TenantCapacityExceeded extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public TenantCapacityExceeded(String tenantId, int capacity) {
            super("concurrent query limit reached for tenant " + tenantId + " (capacity " + capacity + ")");
        }
    }

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

    /**
     * Retires the exact fenced owner and preserves unresolved write recovery.
     * Outstanding read windows can delay a successor.
     */
    boolean release(ExecutionAnchor anchor);

    /** Makes one exact fenced execution immediately eligible for takeover. */
    boolean yield(ExecutionAnchor anchor);

    /** Makes all live anchors for one process immediately eligible for takeover. */
    int yieldOwned(String ownerId, String ownerIncarnation);

    /**
     * Atomically takes over at most {@code maxBatchSize} expired live executions
     * whose read windows have drained, allocating a new token for each claim.
     */
    List<ExecutionAnchor> claimExpired(
            String newOwnerId,
            String newOwnerIncarnation,
            Duration leaseDuration,
            int maxBatchSize);

    /** Returns the unexpired live anchor for an execution, if present. */
    Optional<ExecutionAnchor> current(String executionId);
}
