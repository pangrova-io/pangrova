package org.pangrova.governance.api.audit;

import org.pangrova.governance.api.enums.OperationType;

import java.time.Instant;
import java.util.Map;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Immutable record of a governance operation for the audit trail.
 * Captures who did what, when, and the before/after state.
 *
 * @param id            unique entry identifier
 * @param timestamp     when the operation occurred
 * @param actor         identity of the user or system performing the operation
 * @param operationType category of governance operation (e.g. POLICY_CHANGE, ACCESS_GRANT)
 * @param resourcePath  the resource affected (e.g. catalog.schema.table)
 * @param beforeState   state before the operation (empty map if creation)
 * @param afterState    state after the operation (empty map if deletion)
 * @param details       additional context about the operation
 */
public record GovernanceAuditEntry(
        long id,
        Instant timestamp,
        String actor,
        OperationType operationType,
        String resourcePath,
        Map<String, String> beforeState,
        Map<String, String> afterState,
        String details) {

    public GovernanceAuditEntry {
        checkNotNull(timestamp, "timestamp must not be null");
        checkNotNull(actor, "actor must not be null");
        checkNotNull(operationType, "operationType must not be null");
        checkNotNull(resourcePath, "resourcePath must not be null");
        beforeState = beforeState == null ? Map.of() : Map.copyOf(beforeState);
        afterState = afterState == null ? Map.of() : Map.copyOf(afterState);
        details = details == null ? "" : details;
    }
}
