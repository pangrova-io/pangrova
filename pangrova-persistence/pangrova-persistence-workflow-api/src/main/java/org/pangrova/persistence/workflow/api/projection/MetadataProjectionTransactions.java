package org.pangrova.persistence.workflow.api.projection;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import java.util.Optional;
import java.util.UUID;

/**
 * Thread-safe atomic projection writer. A new operation validates and locks the exact current
 * deployment, reads its previous projection/tenant impact, mutates the projection, and commits
 * durable invalidation and an immutable receipt in one transaction. Before mutation, the immutable
 * request must be durably discoverable by catalog so another process can recover its original ID.
 * No connection escapes.
 * Replaying the same operation ID/content returns its original receipt without another write;
 * differing content or stale deployment is rejected. A commit result that cannot be established
 * must be reported as outcome unknown and confirmed by ID before any new attempt.
 *
 * <p>These are per-operation guarantees, not a scan-wide publication, source-version comparison
 * or lease among concurrent scans of the same deployment. Assembly owns shared backend resources.</p>
 */
@Experimental
public interface MetadataProjectionTransactions {
    /**
     * Synchronous operation; return means the receipt and effects are committed. A historical
     * replay may succeed after the deployment has changed and grants no authority for new work.
     * Invalid targets raise MetadataProjectionScopeException; stale authority or conflicting
     * content raises MetadataProjectionConflictException. Persistence failures are not no-ops.
     * A timeout/cancellation cannot roll back an already committed operation: uncertain outcomes
     * must be confirmed using this request's ID. A failed execution with a durable pending request
     * raises MetadataProjectionPendingException: recovery may replay it, so do not submit a new ID.
     * Implementations must not retain caller resources.
     */
    MetadataProjectionReceipt apply(MetadataProjectionMutation mutation);

    /**
     * Resolves previously accepted intents before a new source observation begins, including after
     * a process restart. Recovery uses each original ID and immutable payload. Implementations also
     * reject new apply IDs behind pending intents; a failed recovery cannot be bypassed by a
     * new ID. This is not a scan lease: concurrent source observations still require source ordering.
     * An obsolete intent may be rejected only under the same current-deployment fence used by writes.
     * Unknown recovery outcomes propagate and must not permit a new scan or a fallback.
     */
    void recoverPending(CatalogDeploymentIdentity deployment);

    /**
     * Returns only a committed receipt visible to this read. Absence is not proof that an
     * in-flight operation failed, and does not authorize a new operation ID. Read failures
     * propagate; they must not be represented as Optional.empty().
     */
    Optional<MetadataProjectionReceipt> findReceipt(UUID operationId);
}
