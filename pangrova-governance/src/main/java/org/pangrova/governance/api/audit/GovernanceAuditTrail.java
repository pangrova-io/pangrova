package org.pangrova.governance.api.audit;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Immutable audit trail for governance operations.
 * Records all policy changes, access grants, quality rule modifications,
 * and tag assignments with timestamp, actor, and before/after state.
 */
public interface GovernanceAuditTrail {

    /**
     * Records a governance operation in the audit trail.
     *
     * @param actor         identity of the user performing the operation
     * @param operationType category of the operation
     * @param resourcePath  affected resource (e.g. "catalog.schema.table.column")
     * @param beforeState   state before the operation (empty map for creation)
     * @param afterState    state after the operation (empty map for deletion)
     * @param details       human-readable description of the change
     * @return the persisted audit entry with assigned ID and timestamp
     */
    GovernanceAuditEntry record(
            String actor,
            OperationType operationType,
            String resourcePath,
            Map<String, String> beforeState,
            Map<String, String> afterState,
            String details);

    /**
     * Retrieves audit entries for a specific resource, ordered by timestamp descending.
     *
     * @param resourcePath the resource path to query
     * @return immutable list of audit entries, most recent first
     */
    List<GovernanceAuditEntry> getEntriesForResource(String resourcePath);

    /**
     * Retrieves audit entries by actor, ordered by timestamp descending.
     *
     * @param actor the actor identity to query
     * @return immutable list of audit entries, most recent first
     */
    List<GovernanceAuditEntry> getEntriesByActor(String actor);

    /**
     * Retrieves audit entries within a time range, ordered by timestamp descending.
     *
     * @param from start of the time range (inclusive)
     * @param to   end of the time range (exclusive)
     * @return immutable list of audit entries, most recent first
     */
    List<GovernanceAuditEntry> getEntriesInRange(Instant from, Instant to);

    /**
     * Returns the total number of audit entries recorded.
     */
    long entryCount();
}
