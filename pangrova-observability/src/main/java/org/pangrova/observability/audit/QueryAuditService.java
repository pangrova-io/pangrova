package org.pangrova.observability.audit;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import java.util.Optional;

/**
 * Manages query audit record lifecycle: persist, retrieve, and cleanup.
 * Delegates storage to a pluggable {@link AuditStore} functional interface.
 */
public final class QueryAuditService {

    private final AuditStore auditStore;

    public QueryAuditService(AuditStore auditStore) {
        this.auditStore = auditStore;
    }

    /**
     * Persists a query audit record. Wraps storage failures with OBS error code.
     */
    public void persist(QueryAuditRecord record) {
        try {
            auditStore.save(record);
        } catch (Exception e) {
            throw new PangrovaException(
                    PangrovaErrorCode.OBS_AUDIT_PERSISTENCE_FAILED,
                    "Failed to persist audit record for query: " + record.queryId(), e);
        }
    }

    /**
     * Retrieves an audit record by query ID.
     */
    public Optional<QueryAuditRecord> findByQueryId(String queryId) {
        return auditStore.findById(queryId);
    }

    /**
     * Removes audit records older than the specified retention period.
     */
    public long cleanupExpired(int retentionDays) {
        return auditStore.deleteOlderThan(retentionDays);
    }

    /**
     * Pluggable persistence abstraction for audit records.
     * Decouples audit service from concrete storage (DB, file, etc.).
     */
    public interface AuditStore {
        void save(QueryAuditRecord record);
        Optional<QueryAuditRecord> findById(String queryId);
        long deleteOlderThan(int retentionDays);
    }
}
